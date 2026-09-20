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
 * [FactSubscriber] and the bounded [FactReplayer]: each of those resolves a start
 * cursor and an [EndBoundary], then delegates to a single shared [scan] pipeline.
 *
 * @param streamBatchSize the number of entries a [FactStreamer] stream reads per transaction
 */
class FdbFactStreamer(
    private val store: FdbFactStore,
    private val deserializationDispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val streamBatchSize: Int = STREAM_BATCH_SIZE,
) : FactStreamer, FactSubscriber, FactReplayer {

    init {
        require(streamBatchSize > 0) { "The stream batch size must be positive, but was $streamBatchSize." }
    }

    // -------------------------------------------------------------------------
    // Stream (bounded, pinned at call time)
    // -------------------------------------------------------------------------

    override suspend fun streamFacts(request: StreamFactsRequest): StreamFactsResult {
        val pinned = pinHead(request.storeName) ?: return StreamFactsResult.StoreNotFound(request.storeName)
        val head = pinned.head ?: return StreamFactsResult.FactStream(emptyFlow())

        // The facts themselves are stored in position order, so they are their own index.
        val factSubspace = store.context.factSubspace
        val facts = scanPinned(
            keys = PinnedKeys(
                range = factSubspace.getRange(pinned.storeId),
                pinnedEndKey = factSubspace.getFactKey(pinned.storeId, head),
                direction = request.direction,
            ),
            limit = request.limit,
        ) { _, entries -> CompletableFuture.completedFuture(entries.map { it.value }) }

        return StreamFactsResult.FactStream(facts)
    }

    override suspend fun streamFactsBySubject(request: StreamFactsBySubjectRequest): StreamFactsBySubjectResult {
        val pinned = pinHead(request.storeName) ?: return StreamFactsBySubjectResult.StoreNotFound(request.storeName)
        val head = pinned.head ?: return StreamFactsBySubjectResult.FactStream(emptyFlow())

        val subjectIndex = store.context.subjectIndexSubspace
        val facts = scanPinned(
            keys = PinnedKeys(
                range = subjectIndex.range(pinned.storeId, request.subject),
                pinnedEndKey = subjectIndex.getKey(pinned.storeId, request.subject, head),
                direction = request.direction,
            ),
            limit = request.limit,
        ) { tr, entries -> loadFacts(tr, pinned.storeId, entries.map { subjectIndex.unpackPosition(it.key) }) }

        return StreamFactsBySubjectResult.FactStream(facts)
    }

    override suspend fun streamFactsByType(request: StreamFactsByTypeRequest): StreamFactsByTypeResult {
        val pinned = pinHead(request.storeName) ?: return StreamFactsByTypeResult.StoreNotFound(request.storeName)
        val head = pinned.head ?: return StreamFactsByTypeResult.FactStream(emptyFlow())

        val typeIndex = store.context.eventTypeIndexSubspace
        val facts = scanPinned(
            keys = PinnedKeys(
                range = typeIndex.range(pinned.storeId, request.type),
                pinnedEndKey = typeIndex.getKey(pinned.storeId, request.type, head),
                direction = request.direction,
            ),
            limit = request.limit,
        ) { tr, entries -> loadFacts(tr, pinned.storeId, entries.map { typeIndex.unpackPosition(it.key) }) }

        return StreamFactsByTypeResult.FactStream(facts)
    }

    override suspend fun streamFactsByTags(request: StreamFactsByTagsRequest): StreamFactsByTagsResult {
        val pinned = pinHead(request.storeName) ?: return StreamFactsByTagsResult.StoreNotFound(request.storeName)
        val head = pinned.head ?: return StreamFactsByTagsResult.FactStream(emptyFlow())

        val tagsIndex = store.context.tagsIndexSubspace
        val tags = request.tags.map { (key, value) -> key to value }

        // One tag is a plain scan of its index; several are an intersection of their scans.
        val facts = if (tags.size == 1) {
            scanPinned(
                keys = tagsIndex.pinnedKeys(pinned.storeId, tags.single(), head, request.direction),
                limit = request.limit,
            ) { tr, entries -> loadFacts(tr, pinned.storeId, entries.map { tagsIndex.unpackPosition(it.key) }) }
        } else {
            val cursors = tags.map { tag ->
                PositionCursor(
                    db = store.db,
                    keys = tagsIndex.pinnedKeys(pinned.storeId, tag, head, request.direction),
                    batchSize = streamBatchSize,
                    positionOf = { key -> tagsIndex.unpackPosition(key) },
                    keyOf = { position -> tagsIndex.getKey(pinned.storeId, tag, position) },
                )
            }
            intersect(cursors, request.direction).loadFacts(pinned.storeId, request.limit)
        }

        return StreamFactsByTagsResult.FactStream(facts)
    }

    /** A store and its head at the moment it was resolved; no head means the store holds no facts. */
    private class PinnedStore(val storeId: StoreId, val head: FactPosition?)

    /** Resolves the store and its current head in one read, or `null` if the store does not exist. */
    private suspend fun pinHead(storeName: StoreName): PinnedStore? =
        read { tr ->
            with(tr) {
                store.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) CompletableFuture.completedFuture(null)
                    else store.getHead(storeId, tr).thenApply { head -> PinnedStore(storeId, head) }
                }
            }
        }

    /**
     * Streams the facts behind [keys], at most [limit] of them.
     *
     * The batches are read in their own coroutine, up to [STREAM_PREFETCH_BATCHES] ahead of the
     * consumer, so reading the next batches overlaps with processing the current one.
     *
     * [load] turns the entries of a batch into serialized facts, within the batch's transaction.
     */
    private fun scanPinned(keys: PinnedKeys, limit: Limit, load: BatchLoader): Flow<Fact> =
        readPinnedBatches(keys, limit, load)
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
    private fun readPinnedBatches(keys: PinnedKeys, limit: Limit, load: BatchLoader): Flow<List<ByteArray>> = flow {
        var cursor: ByteArray? = null
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

        return SubscribeResult.FactStream(scan(storeId, initialCursor, EndBoundary.Follow))
    }

    // -------------------------------------------------------------------------
    // Replay (bounded)
    // -------------------------------------------------------------------------

    override suspend fun replay(request: ReplayRequest): ReplayResult {
        val storeName = request.storeName

        // Resolve the store, the begin cursor and the pinned head in a single
        // consistent read, so the replay window cannot be skewed by concurrent
        // writes or deletes between separate transactions.
        val resolution = read { tr ->
            with(tr) {
                store.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) CompletableFuture.completedFuture(ReplayResolution.StoreMissing)
                    else resolveReplayBounds(tr, storeId, request.start)
                }
            }
        }

        return when (resolution) {
            ReplayResolution.StoreMissing -> ReplayResult.StoreNotFound(storeName)
            is ReplayResolution.CursorMissing -> ReplayResult.FactIdNotFound(resolution.factId)
            is ReplayResolution.Resolved -> {
                // No head => empty store => nothing to replay.
                val pinnedEndKey = resolution.pinnedEndKey ?: return ReplayResult.FactStream(emptyFlow())
                ReplayResult.FactStream(scan(resolution.storeId, resolution.beginCursor, EndBoundary.StopAt(pinnedEndKey)))
            }
        }
    }

    private fun resolveReplayBounds(
        tr: ReadTransaction,
        storeId: StoreId,
        start: ReplayStart,
    ): CompletableFuture<ReplayResolution> {
        val beginFuture: CompletableFuture<BeginResolution> = when (start) {
            ReplayStart.Beginning -> CompletableFuture.completedFuture(BeginResolution.From(null))
            is ReplayStart.After -> with(tr) {
                store.context.factPositionIndexSubspace.getPosition(storeId, start.factId).thenApply { position ->
                    if (position == null) BeginResolution.Missing(start.factId)
                    else BeginResolution.From(store.context.factSubspace.getFactKey(storeId, position))
                }
            }
        }

        return beginFuture.thenCompose { begin ->
            when (begin) {
                is BeginResolution.Missing -> CompletableFuture.completedFuture(ReplayResolution.CursorMissing(begin.factId))
                is BeginResolution.From -> store.getHead(storeId, tr).thenApply { headPosition ->
                    val pinnedEndKey = headPosition?.let { store.context.factSubspace.getFactKey(storeId, it) }
                    ReplayResolution.Resolved(storeId, begin.cursor, pinnedEndKey)
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Shared scan pipeline
    // -------------------------------------------------------------------------

    private fun scan(
        storeId: StoreId,
        beginCursor: ByteArray?,
        endBoundary: EndBoundary,
    ): Flow<List<Fact>> =
        readBatches(storeId, beginCursor, endBoundary)
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
        endBoundary: EndBoundary,
    ): Flow<List<KeyValue>> = flow {
        val globalRange = store.context.factSubspace.getRange(storeId)
        var lastSeenKey = beginCursor
        while (true) {
            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, endBoundary, tr)
                    .thenApply { batch -> batch.toReadResult(tr, storeId, endBoundary) }
            }.await()
            when (readResult) {
                is ReadResult.BatchResult -> {
                    lastSeenKey = readResult.batch.last().key
                    emit(readResult.batch)
                }

                is ReadResult.WatchResult -> readResult.waitForFacts()

                ReadResult.Complete -> return@flow
            }
        }
    }

    private fun readNextBatch(
        lastSeenKey: ByteArray?,
        globalRange: Range,
        endBoundary: EndBoundary,
        tr: ReadTransaction
    ): CompletableFuture<List<KeyValue>> {
        val beginSelector =
            if (lastSeenKey == null)
                KeySelector.firstGreaterOrEqual(globalRange.begin)
            else
                KeySelector.firstGreaterThan(lastSeenKey)

        return tr.snapshot().getRange(
            beginSelector,
            endBoundary.endSelector(globalRange),
            DEFAULT_BATCH_SIZE,
            false,
            StreamingMode.WANT_ALL
        ).asList()
    }

    private fun List<KeyValue>.toReadResult(
        tr: Transaction,
        storeId: StoreId,
        endBoundary: EndBoundary
    ): ReadResult =
        if (isNotEmpty()) {
            ReadResult.BatchResult(this)
        } else when (endBoundary) {
            // Live tail: wait for the next append, then resume.
            EndBoundary.Follow -> {
                val watchFuture = tr.watch(store.context.headSubspace.headKey(storeId))
                ReadResult.WatchResult { watchFuture.await() }
            }
            // Bounded: pinned head reached, the replay is done.
            is EndBoundary.StopAt -> ReadResult.Complete
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
        data object Complete : ReadResult
    }

    /**
     * Where the scan ends. [Follow] tails the live head indefinitely; [StopAt]
     * reads up to and including the head pinned at replay start, then completes.
     */
    private sealed interface EndBoundary {
        fun endSelector(globalRange: Range): KeySelector

        data object Follow : EndBoundary {
            override fun endSelector(globalRange: Range): KeySelector =
                KeySelector.firstGreaterOrEqual(globalRange.end)
        }

        // Plain class (not data class): equals/hashCode over a ByteArray would be
        // identity-based and misleading.
        class StopAt(val pinnedEndKey: ByteArray) : EndBoundary {
            // The pinned end is the head fact's key; include it with firstGreaterThan.
            override fun endSelector(globalRange: Range): KeySelector =
                KeySelector.firstGreaterThan(pinnedEndKey)
        }
    }

    private sealed interface ReplayResolution {
        data object StoreMissing : ReplayResolution
        data class CursorMissing(val factId: FactId) : ReplayResolution
        class Resolved(
            val storeId: StoreId,
            val beginCursor: ByteArray?,
            val pinnedEndKey: ByteArray?,
        ) : ReplayResolution
    }

    private sealed interface BeginResolution {
        class From(val cursor: ByteArray?) : BeginResolution
        data class Missing(val factId: FactId) : BeginResolution
    }

    private suspend fun <T> read(trBlock: (ReadTransaction) -> CompletableFuture<T>): T =
        store.db.readAsync(trBlock).await()
}
