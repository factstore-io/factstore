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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.BufferOverflow.SUSPEND
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.transform
import java.util.concurrent.CompletableFuture

const val DEFAULT_BATCH_SIZE = 10_000
const val RAW_CHANNEL_CAPACITY = 4

/**
 * The number of entries a [FactStreamer] stream reads per transaction.
 *
 * Every batch is read in its own short transaction, far from FoundationDB's five-second
 * limit: even at the largest fact the specification allows, about 75 kB, a batch loads
 * less than 20 MB.
 */
const val STREAM_BATCH_SIZE = 256

/**
 * The number of batches a [FactStreamer] stream reads ahead of its consumer.
 *
 * Reading ahead lets the next batches be read while the consumer processes the current one.
 * The batches read ahead wait in memory: with the batch being processed, a stream holds at
 * most 5 batches, which is about 96 MB at the largest fact size and a few MB for typical facts.
 */
const val STREAM_PREFETCH_BATCHES = 4

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

        // The facts themselves are stored in position order, so they are their own index.
        val factSubspace = store.context.factSubspace
        val facts = scanPinned(
            keys = PinnedKeys(
                range = factSubspace.getRange(pinned.storeId),
                pinnedEndKey = factSubspace.getFactKey(pinned.storeId, head),
                direction = request.direction,
            ),
            continueAfterKey = pinned.continueAfter?.let { factSubspace.getFactKey(pinned.storeId, it) },
            limit = request.limit,
        ) { _, entries -> CompletableFuture.completedFuture(entries.map { it.value }) }

        return StreamFactsResult.FactStream(facts)
    }

    override suspend fun streamFactsBySubject(request: StreamFactsBySubjectRequest): StreamFactsBySubjectResult {
        val pinned = when (val pinned = pin(request.storeName, request.continueAfter)) {
            Pinned.StoreMissing -> return StreamFactsBySubjectResult.StoreNotFound(request.storeName)
            is Pinned.ContinuationMissing -> return StreamFactsBySubjectResult.ContinuationNotFound(pinned.factId)
            is Pinned.Resolved -> pinned
        }
        val head = pinned.head ?: return StreamFactsBySubjectResult.FactStream(emptyFlow())

        val subjectIndex = store.context.subjectIndexSubspace
        val facts = scanPinned(
            keys = subjectIndex.pinnedKeys(pinned.storeId, request.subject, head, request.direction),
            // The continuation marks a position, and needs no entry of its own in this index.
            continueAfterKey = pinned.continueAfter?.let { subjectIndex.getKey(pinned.storeId, request.subject, it) },
            limit = request.limit,
        ) { tr, entries -> loadFacts(tr, pinned.storeId, entries.map { subjectIndex.unpackPosition(it.key) }) }

        return StreamFactsBySubjectResult.FactStream(facts)
    }

    override suspend fun streamFactsByType(request: StreamFactsByTypeRequest): StreamFactsByTypeResult {
        val pinned = when (val pinned = pin(request.storeName, request.continueAfter)) {
            Pinned.StoreMissing -> return StreamFactsByTypeResult.StoreNotFound(request.storeName)
            is Pinned.ContinuationMissing -> return StreamFactsByTypeResult.ContinuationNotFound(pinned.factId)
            is Pinned.Resolved -> pinned
        }
        val head = pinned.head ?: return StreamFactsByTypeResult.FactStream(emptyFlow())

        val typeIndex = store.context.eventTypeIndexSubspace
        val facts = scanPinned(
            keys = typeIndex.pinnedKeys(pinned.storeId, request.type, head, request.direction),
            continueAfterKey = pinned.continueAfter?.let { typeIndex.getKey(pinned.storeId, request.type, it) },
            limit = request.limit,
        ) { tr, entries -> loadFacts(tr, pinned.storeId, entries.map { typeIndex.unpackPosition(it.key) }) }

        return StreamFactsByTypeResult.FactStream(facts)
    }

    override suspend fun streamFactsByTags(request: StreamFactsByTagsRequest): StreamFactsByTagsResult {
        val pinned = when (val pinned = pin(request.storeName, request.continueAfter)) {
            Pinned.StoreMissing -> return StreamFactsByTagsResult.StoreNotFound(request.storeName)
            is Pinned.ContinuationMissing -> return StreamFactsByTagsResult.ContinuationNotFound(pinned.factId)
            is Pinned.Resolved -> pinned
        }
        val head = pinned.head ?: return StreamFactsByTagsResult.FactStream(emptyFlow())

        val tagsIndex = store.context.tagsIndexSubspace
        val tags = request.tags.map { (key, value) -> key to value }

        // One tag is a plain scan of its index; several are an intersection of their scans.
        val facts = if (tags.size == 1) {
            scanPinned(
                keys = tagsIndex.pinnedKeys(pinned.storeId, tags.single(), head, request.direction),
                continueAfterKey = pinned.continueAfter?.let { tagsIndex.getKey(pinned.storeId, tags.single(), it) },
                limit = request.limit,
            ) { tr, entries -> loadFacts(tr, pinned.storeId, entries.map { tagsIndex.unpackPosition(it.key) }) }
        } else {
            positionsOf {
                AllOf(tags.map { tagCursor(pinned.storeId, it, head, request.direction) }, request.direction)
                    .continuingAfter(pinned.continueAfter)
            }.loadFacts(pinned.storeId, request.limit)
        }

        return StreamFactsByTagsResult.FactStream(facts)
    }

    override suspend fun streamFactsByQuery(request: StreamFactsByQueryRequest): StreamFactsByQueryResult {
        val pinned = when (val pinned = pin(request.storeName, request.continueAfter)) {
            Pinned.StoreMissing -> return StreamFactsByQueryResult.StoreNotFound(request.storeName)
            is Pinned.ContinuationMissing -> return StreamFactsByQueryResult.ContinuationNotFound(pinned.factId)
            is Pinned.Resolved -> pinned
        }
        val head = pinned.head ?: return StreamFactsByQueryResult.FactStream(emptyFlow())

        // A filter is an intersection of its predicates, and the query the union of its filters.
        val matches = positionsOf {
            val filters = request.query.filters.map { filter ->
                filter.toSource(pinned.storeId, head, request.direction)
            }
            anyOf(filters, request.direction).continuingAfter(pinned.continueAfter)
        }

        return StreamFactsByQueryResult.FactStream(matches.loadFacts(pinned.storeId, request.limit))
    }

    /**
     * The positions of the facts this filter matches: every predicate it sets must hold, while a
     * predicate holding several values matches any of them.
     */
    private fun FactFilter.toSource(storeId: StoreId, head: FactPosition, direction: ReadDirection): PositionSource {
        val tags = tags.map { (key, value) -> key to value }

        val predicates = buildList {
            if (subjects.isNotEmpty()) {
                add(anyOf(subjects.map { subjectCursor(storeId, it, head, direction) }, direction))
            }

            if (types.isNotEmpty() && tags.isNotEmpty()) {
                // Facts are indexed by type and tag together, and that index holds only the facts
                // carrying both, so reading a tag from it skips every fact of the type without it.
                // Each type read this way costs its own cursor, so tags are folded into the types
                // only as far as that keeps the number of cursors the same.
                if (types.size == 1) {
                    val type = types.single()
                    tags.forEach { add(typeTagCursor(storeId, type, it, head, direction)) }
                } else {
                    val folded = tags.first()
                    add(anyOf(types.map { typeTagCursor(storeId, it, folded, head, direction) }, direction))
                    tags.drop(1).forEach { add(tagCursor(storeId, it, head, direction)) }
                }
            } else {
                if (types.isNotEmpty()) {
                    add(anyOf(types.map { typeCursor(storeId, it, head, direction) }, direction))
                }
                tags.forEach { add(tagCursor(storeId, it, head, direction)) }
            }
        }

        return if (predicates.size == 1) predicates.single() else AllOf(predicates, direction)
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

    /** Combines sources into a union, without wrapping a single one. */
    private fun anyOf(sources: List<PositionSource>, direction: ReadDirection): PositionSource =
        if (sources.size == 1) sources.single() else AnyOf(sources, direction)

    private fun subjectCursor(storeId: StoreId, subject: Subject, head: FactPosition, direction: ReadDirection) =
        store.context.subjectIndexSubspace.let { index ->
            PositionCursor(
                db = store.db,
                keys = index.pinnedKeys(storeId, subject, head, direction),
                batchSize = streamBatchSize,
                positionOf = { key -> index.unpackPosition(key) },
                keyOf = { position -> index.getKey(storeId, subject, position) },
            )
        }

    private fun typeCursor(storeId: StoreId, type: FactType, head: FactPosition, direction: ReadDirection) =
        store.context.eventTypeIndexSubspace.let { index ->
            PositionCursor(
                db = store.db,
                keys = index.pinnedKeys(storeId, type, head, direction),
                batchSize = streamBatchSize,
                positionOf = { key -> index.unpackPosition(key) },
                keyOf = { position -> index.getKey(storeId, type, position) },
            )
        }

    private fun typeTagCursor(
        storeId: StoreId,
        type: FactType,
        tag: Pair<TagKey, TagValue>,
        head: FactPosition,
        direction: ReadDirection,
    ) = store.context.tagsTypeIndexSubspace.let { index ->
        PositionCursor(
            db = store.db,
            keys = index.pinnedKeys(storeId, type, tag, head, direction),
            batchSize = streamBatchSize,
            positionOf = { key -> index.unpackPosition(key) },
            keyOf = { position -> index.getKey(storeId, type, tag, position) },
        )
    }

    private fun tagCursor(storeId: StoreId, tag: Pair<TagKey, TagValue>, head: FactPosition, direction: ReadDirection) =
        store.context.tagsIndexSubspace.let { index ->
            PositionCursor(
                db = store.db,
                keys = index.pinnedKeys(storeId, tag, head, direction),
                batchSize = streamBatchSize,
                positionOf = { key -> index.unpackPosition(key) },
                keyOf = { position -> index.getKey(storeId, tag, position) },
            )
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
     * Continues the source after [position]: the named position is a marker, so a source sitting
     * exactly on it moves past it.
     */
    private suspend fun PositionSource.continuingAfter(position: FactPosition?): PositionSource {
        if (position == null) return this
        seekTo(position)
        if (peek() == position) advance()
        return this
    }

    /**
     * Streams the facts behind [keys], at most [limit] of them.
     *
     * The batches are read in their own coroutine, up to [STREAM_PREFETCH_BATCHES] ahead of the
     * consumer, so reading the next batches overlaps with processing the current one.
     *
     * [load] turns the entries of a batch into serialized facts, within the batch's transaction.
     */
    private fun scanPinned(
        keys: PinnedKeys,
        continueAfterKey: ByteArray?,
        limit: Limit,
        load: BatchLoader,
    ): Flow<Fact> =
        readPinnedBatches(keys, continueAfterKey, limit, load)
            .buffer(capacity = STREAM_PREFETCH_BATCHES, onBufferOverflow = SUSPEND)
            .map { serializedFacts ->
                withContext(deserializationDispatcher) {
                    serializedFacts.map { it.toSerializableFdbFact().toFact() }
                }
            }
            .transform { facts -> facts.forEach { emit(it) } }

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
     * Positions are collected into batches, and each batch is loaded in one transaction, so that
     * matching a sparse intersection does not cost one read round trip per fact. As in
     * [scanPinned], reading runs ahead of the consumer.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun Flow<FactPosition>.loadFacts(storeId: StoreId, limit: Limit): Flow<Fact> =
        (limit.value?.let { take(it) } ?: this)
            .chunked(streamBatchSize)
            .map { positions -> read { tr -> loadFacts(tr, storeId, positions) } }
            .buffer(capacity = STREAM_PREFETCH_BATCHES, onBufferOverflow = SUSPEND)
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
            is CursorResult.FactNotFound -> return SubscribeResult.FactIdNotFound(cursorResult.factId)
            CursorResult.Beginning -> null
        }

        return SubscribeResult.FactStream(scan(storeId, initialCursor))
    }

    // -------------------------------------------------------------------------
    // Shared scan pipeline
    // -------------------------------------------------------------------------

    private fun scan(
        storeId: StoreId,
        beginCursor: ByteArray?,
    ): Flow<List<Fact>> =
        readBatches(storeId, beginCursor)
            .buffer(
                capacity = RAW_CHANNEL_CAPACITY,
                onBufferOverflow = SUSPEND
            )
            .map { keyValues ->
                withContext(deserializationDispatcher) {
                    keyValues.map { it.value.toSerializableFdbFact().toFact() }
                }
            }

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

            is StartPosition.After -> {
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
