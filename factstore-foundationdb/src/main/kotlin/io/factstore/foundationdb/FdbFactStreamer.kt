package io.factstore.foundationdb

import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.StreamingMode
import io.factstore.core.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.await

/**
 * The number of entries a [FactStreamer] stream reads per transaction.
 *
 * Every batch is read in its own short transaction, far from FoundationDB's five-second
 * limit: even at the largest fact the specification allows, about 75 kB, a batch loads
 * less than 20 MB.
 */
internal const val STREAM_BATCH_SIZE = 256

/**
 * The number of batches a [FactStreamer] stream reads ahead of its consumer.
 *
 * Reading ahead lets the next batches be read while the consumer processes the current one.
 * The batches read ahead wait in memory: with the batch being processed, a stream holds at
 * most 5 batches, which is about 96 MB at the largest fact size and a few MB for typical facts.
 */
private const val STREAM_PREFETCH_BATCHES = 4

/** Turns the entries of a batch into the serialized facts behind them, within the batch's transaction. */
private typealias BatchLoader = suspend ReadTransaction.(List<KeyValue>) -> List<ByteArray>

/**
 * Streams the facts of a store up to its head when called: a [FactStreamer] on FoundationDB.
 *
 * A stream pins the store's head when it is opened ([pin]), and then reads the facts, or the
 * index entries of a query, up to that head in batches, each in its own transaction.
 *
 * @param streamBatchSize the number of entries a stream reads per transaction
 */
internal class FdbFactStreamer(
    storage: FdbStorage,
    private val streamBatchSize: Int = STREAM_BATCH_SIZE,
) : FactStreamer, FdbAccess by storage {

    init {
        require(streamBatchSize > 0) { "The stream batch size must be positive, but was $streamBatchSize." }
    }

    /** The sources of a stream read every batch in a transaction of its own. */
    internal val sources = QuerySources(keyspace, KeyReader.inOwnTransactions(storage), streamBatchSize)

    override suspend fun streamFacts(request: StreamFactsRequest): StreamFactsResult {
        val pinned = when (val pinned = pin(request.storeName, request.continueAfter)) {
            Pinned.StoreMissing -> return StreamFactsResult.StoreNotFound(request.storeName)
            is Pinned.ContinuationMissing -> return StreamFactsResult.ContinuationNotFound(pinned.factId)
            is Pinned.Resolved -> pinned
        }
        val head = pinned.head ?: return StreamFactsResult.FactStream(emptyFlow())
        val query = request.query ?: return StreamFactsResult.FactStream(allFacts(pinned, head, request))

        val facts = when (val plan = with(sources) { query.toPlan(pinned.storeId, head, request.direction) }) {
            is QueryPlan.SingleRange -> scanPinned(
                keys = plan.keys,
                // The continuation marks a position, and needs no entry of its own in this index.
                continueAfterKey = pinned.continueAfter?.let(plan.index::keyOf),
                limit = request.limit,
            ) { entries -> loadFacts(pinned.storeId, entries.map { plan.index.positionOf(it.key) }) }

            is QueryPlan.Combined ->
                flow { emitAll(plan.newSource().continuingAfter(pinned.continueAfter).positions()) }
                    .loadFacts(pinned.storeId, request.limit)
        }

        return StreamFactsResult.FactStream(facts)
    }

    /** Every fact of the store up to [head]: the facts are stored in position order, so they are their own index. */
    private fun allFacts(pinned: Pinned.Resolved, head: FactPosition, request: StreamFactsRequest): Flow<Fact> {
        val factSubspace = keyspace.factSubspace
        return scanPinned(
            keys = PinnedKeys(
                range = factSubspace.getRange(pinned.storeId),
                pinnedEndKey = factSubspace.getFactKey(pinned.storeId, head),
                direction = request.direction,
            ),
            continueAfterKey = pinned.continueAfter?.let { factSubspace.getFactKey(pinned.storeId, it) },
            limit = request.limit,
        ) { entries -> entries.map { it.value } }
    }

    /**
     * What a stream needs before it can read: the store, the head it stops at, and the position it
     * continues after.
     */
    private sealed interface Pinned {
        data object StoreMissing : Pinned
        data class ContinuationMissing(val factId: FactId) : Pinned
        class Resolved(val storeId: StoreId, val head: FactPosition?, val continueAfter: FactPosition?) : Pinned
    }

    /**
     * Resolves the store, its current head and the continuation in one read, so that a stream
     * cannot see them from different moments.
     */
    private suspend fun pin(storeName: StoreName, continueAfter: FactId?): Pinned = inReadTransaction {
        val storeId = storeIdOf(storeName) ?: return@inReadTransaction Pinned.StoreMissing
        val head = headOf(storeId)
        val position = continueAfter?.let {
            positionOf(storeId, it)
                ?: return@inReadTransaction Pinned.ContinuationMissing(it)
        }
        Pinned.Resolved(storeId, head, position)
    }

    /**
     * Streams the facts behind [keys], at most [limit] of them.
     *
     * [load] turns the entries of a batch into serialized facts, within the batch's transaction.
     */
    private fun scanPinned(
        keys: PinnedKeys,
        continueAfterKey: ByteArray?,
        limit: Limit,
        load: BatchLoader,
    ): Flow<Fact> =
        readPinnedBatches(keys, continueAfterKey, limit, load).toFacts(STREAM_PREFETCH_BATCHES)

    /**
     * Reads the serialized facts behind [keys], at most [limit] of them, and emits them batch by batch.
     *
     * Each batch is read in its own transaction and emitted only once that transaction is done,
     * so no transaction stays open while a batch waits or is processed. The last key a batch read
     * is the cursor the next batch continues from. Keys are versionstamped and positions only grow,
     * so every batch sees the same entries up to the pinned key: together, the batches read what a
     * single transaction would have read.
     */
    private fun readPinnedBatches(
        keys: PinnedKeys,
        continueAfterKey: ByteArray?,
        limit: Limit,
        load: BatchLoader,
    ): Flow<List<ByteArray>> = flow {
        // The keys are read after the continuation, which is exactly how a batch continues.
        var cursor: ByteArray? = continueAfterKey
        var remaining = limit.value ?: Int.MAX_VALUE
        do {
            val size = minOf(streamBatchSize, remaining)
            val batch = readBatch(keys, cursor, size, load)
            if (batch.facts.isNotEmpty()) emit(batch.facts)
            cursor = batch.entries.lastOrNull()?.key
            remaining -= batch.entries.size
            // A batch smaller than requested means every key up to the pinned one was read.
        } while (batch.entries.size == size && remaining > 0)
    }

    /** Reads, in one transaction, up to [size] of the [keys] that follow [cursor], and the facts behind them. */
    private suspend fun readBatch(keys: PinnedKeys, cursor: ByteArray?, size: Int, load: BatchLoader): Batch =
        inReadTransaction {
            val (begin, end) = keys.remainingAfter(cursor)
            val entries = getRange(begin, end, size, keys.reverse, StreamingMode.WANT_ALL).asList().await()
            Batch(entries, load(entries))
        }

    /** The entries one batch read, and the serialized facts behind them. */
    private class Batch(val entries: List<KeyValue>, val facts: List<ByteArray>)

    /**
     * Loads the facts at the positions, at most [limit] of them.
     *
     * The positions found while a batch is loaded make up the next one, so that a dense match
     * does not cost one read round trip per fact, while a sparse one is not held back until
     * enough positions are found to fill a batch.
     */
    private fun Flow<FactPosition>.loadFacts(storeId: StoreId, limit: Limit): Flow<Fact> =
        (limit.value?.let { take(it) } ?: this)
            .batchedAsReady(streamBatchSize)
            .map { positions -> inReadTransaction { loadFacts(storeId, positions) } }
            .toFacts(STREAM_PREFETCH_BATCHES)

    /**
     * Loads the serialized facts at [positions], in their order.
     *
     * Every read is issued before the first is awaited, so they all run at once and the batch
     * costs one round trip rather than one per fact.
     */
    context(tr: ReadTransaction)
    private suspend fun loadFacts(storeId: StoreId, positions: List<FactPosition>): List<ByteArray> {
        val reads = positions.map { position -> position to keyspace.factSubspace.findFact(storeId, position) }
        return reads.map { (position, read) ->
            // An index entry and its fact are written, and removed, in the same transaction.
            read.await() ?: error("Store $storeId has an index entry at $position without a fact.")
        }
    }
}
