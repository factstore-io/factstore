package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.StreamingMode
import com.apple.foundationdb.Transaction
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import io.factstore.core.*
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.BufferOverflow.SUSPEND
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.transform
import java.util.concurrent.CompletableFuture

private const val DEFAULT_BATCH_SIZE = 10_000
private const val RAW_CHANNEL_CAPACITY = 4

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
private typealias BatchLoader = (ReadTransaction, List<KeyValue>) -> CompletableFuture<List<ByteArray>>

/**
 * FoundationDB streaming engine.
 *
 * Backs the bounded [FactStreamer], which pins the store's head when called and then
 * scans an index up to it in batches ([scanPinned]), as well as the live
 * [FactSubscriber], which resolves a start cursor and then follows the head
 * indefinitely ([scan]).
 *
 * @param streamBatchSize the number of entries a [FactStreamer] stream reads per transaction
 */
class FdbFactStreamer(
    private val store: FdbFactStore,
    private val deserializationDispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val streamBatchSize: Int = STREAM_BATCH_SIZE,
) : FactStreamer, FactSubscriber {

    init {
        require(streamBatchSize > 0) { "The stream batch size must be positive, but was $streamBatchSize." }
    }

    /** The sources of a stream read every batch in a transaction of its own. */
    internal val sources = QuerySources(store.context, KeyReader.inOwnTransactions(store.db), streamBatchSize)

    // -------------------------------------------------------------------------
    // Stream (bounded, pinned at call time)
    // -------------------------------------------------------------------------

    override suspend fun streamFacts(request: StreamFactsRequest): StreamFactsResult {
        val pinned = when (val pinned = pin(request.storeName, request.continueAfter)) {
            Pinned.StoreMissing -> return StreamFactsResult.StoreNotFound(request.storeName)
            is Pinned.ContinuationMissing -> return StreamFactsResult.ContinuationNotFound(pinned.factId)
            is Pinned.Resolved -> pinned
        }
        val head = pinned.head ?: return StreamFactsResult.FactStream(emptyFlow())
        val query = request.query ?: return StreamFactsResult.FactStream(allFacts(pinned, head, request))

        val source = { with(sources) { query.toSource(pinned.storeId, head, request.direction) } }

        // A query that comes down to a single index range needs nothing combined: the range is
        // scanned as it is, reading index entries and facts together. That is the case for one
        // subject, one type, one tag, or one type and tag.
        val range = source() as? PositionCursor
        val facts = if (range != null) {
            scanPinned(
                keys = range.keys,
                // The continuation marks a position, and needs no entry of its own in this index.
                continueAfterKey = pinned.continueAfter?.let(range.keyOf),
                limit = request.limit,
            ) { tr, entries -> loadFacts(tr, pinned.storeId, entries.map { range.positionOf(it.key) }) }
        } else {
            positionsOf { source().continuingAfter(pinned.continueAfter) }.loadFacts(pinned.storeId, request.limit)
        }

        return StreamFactsResult.FactStream(facts)
    }

    /** Every fact of the store up to [head]: the facts are stored in position order, so they are their own index. */
    private fun allFacts(pinned: Pinned.Resolved, head: FactPosition, request: StreamFactsRequest): Flow<Fact> {
        val factSubspace = store.context.factSubspace
        return scanPinned(
            keys = PinnedKeys(
                range = factSubspace.getRange(pinned.storeId),
                pinnedEndKey = factSubspace.getFactKey(pinned.storeId, head),
                direction = request.direction,
            ),
            continueAfterKey = pinned.continueAfter?.let { factSubspace.getFactKey(pinned.storeId, it) },
            limit = request.limit,
        ) { _, entries -> CompletableFuture.completedFuture(entries.map { it.value }) }
    }

    /**
     * The positions of a source that is built anew for every collection.
     *
     * Sources are cursors, and a cursor is consumed as it is read. Building the tree inside the
     * flow is what keeps a stream repeatable: collecting it twice reads the same facts, rather than
     * finding the cursors of the first collection used up.
     */
    private fun positionsOf(source: suspend () -> PositionSource): Flow<FactPosition> =
        flow { emitAll(source().positions()) }

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
    private suspend fun pin(storeName: StoreName, continueAfter: FactId?): Pinned =
        read { tr ->
            with(tr) {
                store.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(Pinned.StoreMissing)
                    } else {
                        store.getHead(storeId, tr).thenCompose { head ->
                            if (continueAfter == null) {
                                CompletableFuture.completedFuture(Pinned.Resolved(storeId, head, null))
                            } else {
                                store.context.factPositionIndexSubspace.getPosition(storeId, continueAfter)
                                    .thenApply { position ->
                                        if (position == null) Pinned.ContinuationMissing(continueAfter)
                                        else Pinned.Resolved(storeId, head, position)
                                    }
                            }
                        }
                    }
                }
            }
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
        readPinnedBatches(keys, continueAfterKey, limit, load).toFacts(readAhead = STREAM_PREFETCH_BATCHES)

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
        read { tr ->
            val (begin, end) = keys.remainingAfter(cursor)
            tr.getRange(begin, end, size, keys.reverse, StreamingMode.WANT_ALL).asList()
                .thenCompose { entries -> load(tr, entries).thenApply { facts -> Batch(entries, facts) } }
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
            .map { positions -> read { tr -> loadFacts(tr, storeId, positions) } }
            .toFacts(readAhead = STREAM_PREFETCH_BATCHES)

    /**
     * Turns batches of serialized facts into facts.
     *
     * The batches are read in their own coroutine, up to [readAhead] of them ahead of the
     * consumer, so reading the next batches overlaps with processing the current one.
     *
     * Batches are how a store is read, not how its facts are consumed: each fact is emitted on
     * its own, and batching is left to whoever transports it.
     */
    private fun Flow<List<ByteArray>>.toFacts(readAhead: Int): Flow<Fact> =
        buffer(capacity = readAhead, onBufferOverflow = SUSPEND)
            .map { serializedFacts ->
                withContext(deserializationDispatcher) {
                    serializedFacts.map { it.toSerializableFdbFact().toFact() }
                }
            }
            .transform { facts -> facts.forEach { emit(it) } }

    /** Loads the serialized facts at [positions], in their order. */
    private fun loadFacts(
        tr: ReadTransaction,
        storeId: StoreId,
        positions: List<FactPosition>,
    ): CompletableFuture<List<ByteArray>> {
        val futures = positions.map { position ->
            with(tr) { store.context.factSubspace.findFact(storeId, position) }.thenApply { fact ->
                // An index entry and its fact are written, and removed, in the same transaction.
                fact ?: throw IllegalStateException("Store $storeId has an index entry at $position without a fact.")
            }
        }
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply { futures.map { it.resultNow() } }
    }

    // -------------------------------------------------------------------------
    // Subscribe (live tail)
    // -------------------------------------------------------------------------

    override suspend fun subscribe(request: SubscribeRequest): SubscribeResult {
        val storeName = request.storeName

        val storeId = read { tr ->
            with(tr) { store.context.lookUpStoreIdByName(storeName) }
        } ?: return SubscribeResult.StoreNotFound(storeName)

        val cursorResult = with(storeId) { resolveInitialCursor(request.startPosition) }
        val initialCursor = when (cursorResult) {
            is CursorResult.Found -> cursorResult.key
            is CursorResult.FactNotFound -> return SubscribeResult.ContinuationNotFound(cursorResult.factId)
            CursorResult.Beginning -> null
        }

        return SubscribeResult.FactStream(scan(storeId, initialCursor))
    }

    // -------------------------------------------------------------------------
    // Shared scan pipeline
    // -------------------------------------------------------------------------

    /**
     * Reads the store's facts from [beginCursor] onwards in batches, waiting for the next append
     * whenever it runs out.
     *
     * The facts are read in the store's own order, which is the order they are stored in, so a
     * subscription needs no index.
     */
    private fun scan(
        storeId: StoreId,
        beginCursor: ByteArray?,
    ): Flow<Fact> =
        readBatches(storeId, beginCursor)
            .map { keyValues -> keyValues.map { it.value } }
            .toFacts(readAhead = RAW_CHANNEL_CAPACITY)

    private fun readBatches(
        storeId: StoreId,
        beginCursor: ByteArray?,
    ): Flow<List<KeyValue>> = flow {
        val globalRange = store.context.factSubspace.getRange(storeId)
        var lastSeenKey = beginCursor
        while (true) {
            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, tr)
                    .thenApply { batch -> batch.toReadResult(tr, storeId) }
            }.await()
            when (readResult) {
                is ReadResult.BatchResult -> {
                    lastSeenKey = readResult.batch.last().key
                    emit(readResult.batch)
                }

                is ReadResult.WatchResult -> readResult.waitForFacts()
            }
        }
    }

    private fun readNextBatch(
        lastSeenKey: ByteArray?,
        globalRange: Range,
        tr: ReadTransaction
    ): CompletableFuture<List<KeyValue>> {
        val beginSelector =
            if (lastSeenKey == null)
                KeySelector.firstGreaterOrEqual(globalRange.begin)
            else
                KeySelector.firstGreaterThan(lastSeenKey)

        return tr.snapshot().getRange(
            beginSelector,
            KeySelector.firstGreaterOrEqual(globalRange.end),
            DEFAULT_BATCH_SIZE,
            false,
            StreamingMode.WANT_ALL
        ).asList()
    }

    /** An empty batch means the live tail is caught up: wait for the next append, then resume. */
    private fun List<KeyValue>.toReadResult(tr: Transaction, storeId: StoreId): ReadResult =
        if (isNotEmpty()) {
            ReadResult.BatchResult(this)
        } else {
            val watchFuture = tr.watch(store.context.headSubspace.headKey(storeId))
            ReadResult.WatchResult { watchFuture.await() }
        }

    // -------------------------------------------------------------------------
    // Cursor handling (subscribe)
    // -------------------------------------------------------------------------

    sealed interface CursorResult {
        data object Beginning : CursorResult

        @JvmInline
        value class Found(val key: ByteArray) : CursorResult

        @JvmInline
        value class FactNotFound(val factId: FactId) : CursorResult
    }

    context(storeId: StoreId)
    private suspend fun resolveInitialCursor(startPosition: StartPosition): CursorResult =
        when (startPosition) {
            StartPosition.Beginning -> CursorResult.Beginning
            StartPosition.End -> {
                val key = getCurrentEndKey()
                if (key == null) CursorResult.Beginning else CursorResult.Found(key)
            }

            is StartPosition.ContinueAfter -> {
                val key = getKeyForFactOrNull(startPosition.factId)
                if (key == null) CursorResult.FactNotFound(startPosition.factId)
                else CursorResult.Found(key)
            }
        }

    context(storeId: StoreId)
    private suspend fun getKeyForFactOrNull(factId: FactId): ByteArray? =
        read { tr ->
            with(tr) {
                store.context.factPositionIndexSubspace.getPosition(storeId, factId)
                    .thenApply { position ->
                        position?.let { store.context.factSubspace.getFactKey(storeId, it) }
                    }
            }
        }

    context(storeId: StoreId)
    private suspend fun getCurrentEndKey(): ByteArray? =
        read { tr ->
            store.getHead(storeId, tr)
                .thenApply { position ->
                    position?.let { store.context.factSubspace.getFactKey(storeId, it) }
                }
        }

    // -------------------------------------------------------------------------
    // Internal types
    // -------------------------------------------------------------------------

    private sealed interface ReadResult {
        data class BatchResult(val batch: List<KeyValue>) : ReadResult
        data class WatchResult(val waitForFacts: suspend () -> Unit) : ReadResult
    }

    private suspend fun <T> read(trBlock: (ReadTransaction) -> CompletableFuture<T>): T =
        store.db.readAsync(trBlock).await()
}
