package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.StreamingMode
import io.factstore.core.*
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.distinctUntilChangedBy
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import java.util.concurrent.CompletableFuture

/** How many index positions to batch into a single point-read transaction when loading facts. */
private const val LOAD_BATCH_SIZE = 1_000

/**
 * Streaming engine for the unified [FactFinder.query] API on FoundationDB.
 *
 * The result stream is always ordered by global append position (ascending for
 * [ReadDirection.Forward], descending for [ReadDirection.Backward]). Each secondary index is
 * keyed `(storeId, attribute…, versionstamp)`, so a range scan over a fixed attribute prefix
 * yields [FactPosition]s already in that order. Selective queries therefore scan only the
 * relevant index ranges rather than the whole store, and multiple ordered scans are combined
 * with a streaming [mergeSortedBy] k-way merge.
 *
 * See [FdbFactStreamer] for the sibling replay/subscribe engine that this mirrors.
 */
class FdbFactQueryEngine(
    private val store: FdbFactStore,
    private val deserializationDispatcher: CoroutineDispatcher = Dispatchers.Default,
) {

    private val db = store.db
    private val context = store.context

    suspend fun query(request: FactQueryRequest): FactQueryResult {
        val storeId = read { tr -> with(tr) { context.lookUpStoreIdByName(request.storeName) } }
            ?: return FactQueryResult.StoreNotFound(request.storeName)

        // Resolve the exclusive `after` cursor to its position; unknown id => error.
        val cursor: FactPosition? = request.after?.let { after ->
            read { tr -> with(tr) { context.factPositionIndexSubspace.getPosition(storeId, after) } }
                ?: return FactQueryResult.FactIdNotFound(after)
        }

        // Pin the head once so concurrent appends during the paged scan cannot leak in.
        // No head => empty store => empty result.
        val head = read { tr -> store.getHead(storeId, tr) }
            ?: return FactQueryResult.FactStream(emptyFlow())

        val bounds = ScanBounds(cursor, head, request.direction)

        val facts: Flow<Fact> = when (val query = request.query) {
            FactQuery.All -> globalFactStream(storeId, bounds).map { it.fact }
            is FactQuery.AnyOf -> anyOfStream(storeId, query.filters, bounds)
        }

        val limited = request.limit.value?.let { facts.take(it) } ?: facts
        return FactQueryResult.FactStream(limited)
    }

    // -------------------------------------------------------------------------
    // Query composition
    // -------------------------------------------------------------------------

    /** OR across filters: merge each filter's ordered stream and drop duplicate positions. */
    private fun anyOfStream(storeId: StoreId, filters: List<FactFilter>, bounds: ScanBounds): Flow<Fact> {
        val perFilter = filters.map { filterStream(storeId, it, bounds) }
        val merged = perFilter.singleOrNull() ?: perFilter.mergeSortedBy(bounds.direction.toFdbFactComparator())
        return merged
            .distinctUntilChangedBy { it.factPosition }
            .map { it.fact }
    }

    /**
     * A single [FactFilter] as an ordered stream. Drives the scan on the most selective index
     * available for the predicates present, then loads each candidate and applies the full
     * [FactFilter.matches] to enforce any predicate the driving index did not cover.
     */
    private fun filterStream(storeId: StoreId, filter: FactFilter, bounds: ScanBounds): Flow<FdbFact> {
        val driving: Flow<FdbFact> = when {
            filter.subjects.isNotEmpty() ->
                filter.subjects
                    .map { subject -> positions(subjectIndex(storeId, subject), bounds) }
                    .mergedFacts(storeId, bounds)

            filter.tags.isNotEmpty() -> {
                val tag = filter.tags.entries.first().let { it.key to it.value }
                val sources = if (filter.types.isNotEmpty()) {
                    filter.types.map { type -> positions(tagsTypeIndex(storeId, type, tag), bounds) }
                } else {
                    listOf(positions(tagsIndex(storeId, tag), bounds))
                }
                sources.mergedFacts(storeId, bounds)
            }

            filter.types.isNotEmpty() ->
                filter.types
                    .map { type -> positions(eventTypeIndex(storeId, type), bounds) }
                    .mergedFacts(storeId, bounds)

            // Only a time predicate: no position-ordered index exists for it, so fall back to
            // the global log and filter by timestamp while scanning.
            else -> globalFactStream(storeId, bounds)
        }
        return driving.filter { filter.matches(it.fact) }
    }

    /** Merge ordered position streams, drop duplicates, and load the facts in order. */
    private fun List<Flow<FactPosition>>.mergedFacts(storeId: StoreId, bounds: ScanBounds): Flow<FdbFact> =
        (singleOrNull() ?: mergeSortedBy(bounds.direction.toComparator()))
            .distinctUntilChangedBy { it }
            .loadFacts(storeId)

    // -------------------------------------------------------------------------
    // Index scans
    // -------------------------------------------------------------------------

    /** Streams the positions of one index range, paged across short transactions, in order. */
    private fun positions(index: DrivingIndex, bounds: ScanBounds): Flow<FactPosition> =
        pagedRange(bounds.beginSelector(index), bounds.endSelector(index), bounds.direction.isReverse())
            .map { index.unpack(it.key) }

    /** Streams the whole store's facts directly from the global log, in order. */
    private fun globalFactStream(storeId: StoreId, bounds: ScanBounds): Flow<FdbFact> {
        val index = globalIndex(storeId)
        return pagedRange(bounds.beginSelector(index), bounds.endSelector(index), bounds.direction.isReverse())
            .map { kv ->
                withContext(deserializationDispatcher) {
                    FdbFact(kv.value.toSerializableFdbFact().toFact(), index.unpack(kv.key))
                }
            }
            .buffer(RAW_CHANNEL_CAPACITY)
    }

    /** Pages an FDB range in the given direction, tracking a continuation cursor per batch. */
    private fun pagedRange(begin: KeySelector, end: KeySelector, reverse: Boolean): Flow<KeyValue> = flow {
        var beginSelector = begin
        var endSelector = end
        while (true) {
            val batch = db.readAsync { tr ->
                tr.snapshot().getRange(beginSelector, endSelector, DEFAULT_BATCH_SIZE, reverse, StreamingMode.WANT_ALL).asList()
            }.await()

            if (batch.isEmpty()) return@flow
            batch.forEach { emit(it) }

            val lastKey = batch.last().key
            if (reverse) endSelector = KeySelector.firstGreaterOrEqual(lastKey)
            else beginSelector = KeySelector.firstGreaterThan(lastKey)
        }
    }

    /** Loads facts for an ordered position stream, batching point-reads while preserving order. */
    private fun Flow<FactPosition>.loadFacts(storeId: StoreId): Flow<FdbFact> =
        chunked(LOAD_BATCH_SIZE)
            .map { batch ->
                withContext(deserializationDispatcher) {
                    db.readAsync { tr ->
                        val snapshot = tr.snapshot()
                        val futures = batch.map { position ->
                            with(snapshot) {
                                context.factSubspace.findFact(storeId, position).thenApply { bytes ->
                                    bytes?.let { FdbFact(it.toSerializableFdbFact().toFact(), position) }
                                }
                            }
                        }
                        CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                            futures.mapNotNull { it.getNow(null) }
                        }
                    }.await()
                }
            }
            .buffer(RAW_CHANNEL_CAPACITY)
            .transform { facts -> facts.forEach { emit(it) } }

    // -------------------------------------------------------------------------
    // Driving-index descriptors
    // -------------------------------------------------------------------------

    /**
     * A fixed index prefix that can be range-scanned, bounded at a specific [FactPosition]
     * (for cursor/head bounds), and unpacked back into positions.
     */
    private class DrivingIndex(
        val range: Range,
        val keyAt: (FactPosition) -> ByteArray,
        val unpack: (ByteArray) -> FactPosition,
    )

    private fun globalIndex(storeId: StoreId) = DrivingIndex(
        range = context.factSubspace.getRange(storeId),
        keyAt = { context.factSubspace.getFactKey(storeId, it) },
        unpack = { context.factSubspace.unpackPosition(it) },
    )

    private fun subjectIndex(storeId: StoreId, subject: Subject) = DrivingIndex(
        range = context.subjectIndexSubspace.range(storeId, subject),
        keyAt = { context.subjectIndexSubspace.getKey(storeId, subject, it) },
        unpack = { context.subjectIndexSubspace.unpackPosition(it) },
    )

    private fun eventTypeIndex(storeId: StoreId, type: FactType) = DrivingIndex(
        range = context.eventTypeIndexSubspace.range(storeId, type),
        keyAt = { context.eventTypeIndexSubspace.getKey(storeId, type, it) },
        unpack = { context.eventTypeIndexSubspace.unpackPosition(it) },
    )

    private fun tagsIndex(storeId: StoreId, tag: Pair<TagKey, TagValue>) = DrivingIndex(
        range = context.tagsIndexSubspace.range(storeId, tag),
        keyAt = { context.tagsIndexSubspace.getKey(storeId, tag, it) },
        unpack = { context.tagsIndexSubspace.unpackPosition(it) },
    )

    private fun tagsTypeIndex(storeId: StoreId, type: FactType, tag: Pair<TagKey, TagValue>) = DrivingIndex(
        range = context.tagsTypeIndexSubspace.range(storeId, type, tag),
        keyAt = { context.tagsTypeIndexSubspace.getKey(storeId, type, tag, it) },
        unpack = { context.tagsTypeIndexSubspace.unpackPosition(it) },
    )

    // -------------------------------------------------------------------------
    // Scan bounds
    // -------------------------------------------------------------------------

    /**
     * The window every scan is constrained to: strictly past the exclusive [cursor] on the
     * read-direction side, and never beyond the pinned [head].
     */
    private class ScanBounds(
        val cursor: FactPosition?,
        val head: FactPosition,
        val direction: ReadDirection,
    ) {
        fun beginSelector(index: DrivingIndex): KeySelector =
            if (direction.isReverse()) {
                KeySelector.firstGreaterOrEqual(index.range.begin)
            } else {
                cursor?.let { KeySelector.firstGreaterThan(index.keyAt(it)) }
                    ?: KeySelector.firstGreaterOrEqual(index.range.begin)
            }

        fun endSelector(index: DrivingIndex): KeySelector =
            if (direction.isReverse()) {
                // Backward: exclude the cursor and everything after it; otherwise include the head.
                cursor?.let { KeySelector.firstGreaterOrEqual(index.keyAt(it)) }
                    ?: KeySelector.firstGreaterThan(index.keyAt(head))
            } else {
                // Forward: include the head fact, exclude anything appended after it.
                KeySelector.firstGreaterThan(index.keyAt(head))
            }
    }

    private suspend fun <T> read(block: (ReadTransaction) -> CompletableFuture<T>): T =
        db.readAsync(block).await()
}
