package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.StreamingMode
import io.factstore.core.*
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import com.apple.foundationdb.tuple.Tuple
import java.util.concurrent.CompletableFuture

/**
 * FoundationDB-backed query executor for [FactQuery].
 *
 * Decomposes a [FactFilter] into index branches, executes each against the
 * appropriate FDB index, merges results by versionstamp, and streams
 * [Fact] batches to the caller.
 *
 * ### Execution model
 *
 * - [FactFilter.All] → multi-transaction forward/backward full-store scan,
 *   identical to [FdbFactStreamer] but without live-tail support.
 * - [FactFilter.Predicate] → collect-all-positions strategy: index branches
 *   are run in a single read transaction, the resulting [FactPosition] sets
 *   are union-merged and deduplicated, then facts are loaded in
 *   [DEFAULT_BATCH_SIZE] batches via subsequent read transactions.
 * - [FactFilter.Predicate.Last]/[FactFilter.Predicate.First] → bounded
 *   reverse/forward scan of the inner predicate's index; the inner predicate
 *   is guaranteed self-contained after [FactFilter.withNormalizedBoundedSelectors].
 *
 * @author Domenic Cassisi
 */
class FdbFactQuerier(
    private val store: FdbFactStore,
    private val deserializationDispatcher: CoroutineDispatcher = Dispatchers.Default,
) {
    private val ctx get() = store.context

    // ── Public API ────────────────────────────────────────────────────────────

    suspend fun query(query: FactQuery): FactQueryResult {
        val storeId = read { tr ->
            with(tr) { ctx.lookUpStoreIdByName(query.storeName) }
        } ?: return FactQueryResult.StoreNotFound(query.storeName)

        val afterPosition = query.cursor?.let { factId ->
            read { tr ->
                with(tr) { ctx.factPositionIndexSubspace.getPosition(storeId, factId) }
            } ?: return FactQueryResult.CursorNotFound(factId)
        }

        // Normalize here rather than relying on the caller (e.g. the factQuery DSL) to
        // have done so: gRPC/HTTP requests construct FactFilter trees directly from the
        // wire, bypassing the DSL entirely, so this is the only place guaranteed to see
        // every query regardless of origin. Without this, ancestor AllOf leaf constraints
        // are silently dropped when collectPositions recurses into a nested First/Last.
        val normalizedFilter = query.filter.withNormalizedBoundedSelectors()
        val stream = buildStream(storeId, normalizedFilter, afterPosition, query.direction, query.limit)
        return FactQueryResult.FactStream(stream)
    }

    // ── Stream construction ───────────────────────────────────────────────────

    private fun buildStream(
        storeId: StoreId,
        filter: FactFilter,
        afterPosition: FactPosition?,
        direction: ReadDirection,
        limit: Limit,
    ): Flow<List<Fact>> = when (filter) {
        FactFilter.All -> fullStoreStream(storeId, afterPosition, direction, limit)
        is FactFilter.Predicate -> predicateStream(storeId, filter, afterPosition, direction, limit)
    }

    // ── Full-store streaming scan (FactFilter.All) ────────────────────────────

    private fun fullStoreStream(
        storeId: StoreId,
        afterPosition: FactPosition?,
        direction: ReadDirection,
        limit: Limit,
    ): Flow<List<Fact>> {
        val isReversed = direction == ReadDirection.Backward
        val globalRange = ctx.factSubspace.getRange(storeId)

        return flow {
            var cursor: ByteArray? = afterPosition?.let { ctx.factSubspace.getFactKey(storeId, it) }
            var remaining = limit.value

            while (remaining == null || remaining > 0) {
                val batchSize = remaining?.coerceAtMost(DEFAULT_BATCH_SIZE) ?: DEFAULT_BATCH_SIZE
                val batch = read { tr ->
                    // getRange always takes begin <= end (inclusive, exclusive) regardless
                    // of the reverse flag — reverse only changes iteration/truncation order
                    // within that same interval. The cursor narrows whichever side of the
                    // interval faces the scan direction: it's a lower bound when scanning
                    // forward, an upper bound when scanning backward.
                    val begin = if (!isReversed && cursor != null)
                        KeySelector.firstGreaterThan(cursor!!)
                    else
                        KeySelector.firstGreaterOrEqual(globalRange.begin)

                    val end = if (isReversed && cursor != null)
                        KeySelector.firstGreaterOrEqual(cursor!!)
                    else
                        KeySelector.firstGreaterOrEqual(globalRange.end)

                    tr.snapshot().getRange(begin, end, batchSize, isReversed, StreamingMode.WANT_ALL).asList()
                }

                if (batch.isEmpty()) break
                cursor = batch.last().key
                remaining = remaining?.minus(batch.size)

                val facts = withContext(deserializationDispatcher) {
                    batch.map { it.value.toSerializableFdbFact().toFact() }
                }
                emit(facts)
            }
        }.buffer(capacity = RAW_CHANNEL_CAPACITY)
    }

    // ── Predicate-based streaming ─────────────────────────────────────────────

    private fun predicateStream(
        storeId: StoreId,
        predicate: FactFilter.Predicate,
        afterPosition: FactPosition?,
        direction: ReadDirection,
        limit: Limit,
    ): Flow<List<Fact>> = flow {
        val positions = collectPositions(storeId, predicate, afterPosition, direction)
        if (positions.isEmpty()) return@flow

        val comparator = direction.toComparator()
        val sorted = positions.toSortedSet(comparator)
        val limited = limit.value?.let { sorted.take(it) } ?: sorted.toList()
        if (limited.isEmpty()) return@flow

        for (chunk in limited.chunked(DEFAULT_BATCH_SIZE)) {
            val facts = read { tr ->
                val futures = chunk.map { pos ->
                    with(tr) {
                        with(store) { storeId.run { pos.loadFactByPosition() } }
                    }
                }
                CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                    futures.mapNotNull { it.resultNow()?.fact }
                }
            }
            if (facts.isNotEmpty()) emit(facts)
        }
    }.buffer(capacity = RAW_CHANNEL_CAPACITY)

    // ── Position collection (recursive predicate decomposition) ───────────────

    /**
     * Recursively collects all [FactPosition]s matching [predicate], taking
     * [outerLeaves] as additional AND-constraints injected from ancestor
     * [FactFilter.Predicate.AllOf] nodes.
     *
     * Results are returned in an arbitrary order; the caller sorts and deduplicates.
     */
    private suspend fun collectPositions(
        storeId: StoreId,
        predicate: FactFilter.Predicate,
        afterPosition: FactPosition?,
        direction: ReadDirection,
        outerLeaves: List<FactFilter.Predicate> = emptyList(),
        // Only set when this call is known to represent a single, self-contained branch
        // (i.e. we're inside a First/Last bounded scan) — see collectBoundedPositions.
        // A union of branches (AnyOf) may still safely inherit it (the top-n of a union
        // is always a subset of the top-n of each branch), but an intersection (AllOf
        // with multiple non-leaf children) may not, so it is dropped there.
        hardLimit: Int? = null,
    ): List<FactPosition> = when (predicate) {

        is FactFilter.Predicate.AnyOf -> {
            coroutineScope {
                predicate.predicates
                    .map { child -> async { collectPositions(storeId, child, afterPosition, direction, outerLeaves, hardLimit) } }
                    .flatMap { it.await() }
            }
        }

        is FactFilter.Predicate.AllOf -> {
            val myLeaves = predicate.predicates.filter { it.isLeafPredicate() }
            val combined = outerLeaves + myLeaves
            val nonLeaves = predicate.predicates.filter { !it.isLeafPredicate() }

            when {
                nonLeaves.isEmpty() ->
                    collectLeafPositions(storeId, combined, afterPosition, direction, hardLimit)

                nonLeaves.size == 1 ->
                    collectPositions(storeId, nonLeaves[0], afterPosition, direction, combined, hardLimit)

                else -> {
                    // Multiple non-leaf children → AND semantics → intersect position sets.
                    // A per-branch hard limit is unsafe here: the top-n of each branch is
                    // not guaranteed to contain the top-n of their intersection.
                    val sets = coroutineScope {
                        nonLeaves.map { child ->
                            async { collectPositions(storeId, child, afterPosition, direction, combined, hardLimit = null).toHashSet() }
                        }.map { it.await() }
                    }
                    sets.reduce { acc, set -> (acc intersect set).toHashSet() }.toList()
                }
            }
        }

        is FactFilter.Predicate.Last ->
            collectBoundedPositions(storeId, predicate.n, predicate.predicate, ReadDirection.Backward, afterPosition)

        is FactFilter.Predicate.First ->
            collectBoundedPositions(storeId, predicate.n, predicate.predicate, ReadDirection.Forward, afterPosition)

        else ->
            // Leaf predicate — combine with outer leaves
            collectLeafPositions(storeId, outerLeaves + predicate, afterPosition, direction, hardLimit)
    }

    /**
     * Collects positions for a bounded selector ([FactFilter.Predicate.Last]/[FactFilter.Predicate.First]).
     * Scans the inner predicate's index in [boundDirection], taking up to [n] matches.
     *
     * [n] is threaded down as a hard limit so that, whenever the inner predicate resolves
     * to a single index scan with no in-app residual filter, the FDB range read itself is
     * bounded and reversed appropriately — turning e.g. `last(1) { type("Activated") }`
     * into a genuine O(1) reverse point-scan instead of an unbounded forward scan of the
     * entire matching index followed by an in-application sort and truncation.
     */
    private suspend fun collectBoundedPositions(
        storeId: StoreId,
        n: Int,
        predicate: FactFilter.Predicate,
        boundDirection: ReadDirection,
        afterPosition: FactPosition?,
    ): List<FactPosition> =
        collectPositions(storeId, predicate, afterPosition, boundDirection, hardLimit = n)
            .toSortedSet(boundDirection.toComparator())
            .take(n)

    /**
     * Collects positions for a set of leaf-predicate constraints. Selects the
     * most efficient FDB index based on predicate types, then applies any
     * remaining predicates as an in-application residual filter.
     *
     * [direction] determines which side of [afterPosition] is scanned and, together
     * with [hardLimit], whether the underlying FDB range read can be bounded and
     * reversed natively rather than fetched in full and sorted in application code.
     */
    private suspend fun collectLeafPositions(
        storeId: StoreId,
        leaves: List<FactFilter.Predicate>,
        afterPosition: FactPosition?,
        direction: ReadDirection = ReadDirection.Forward,
        hardLimit: Int? = null,
    ): List<FactPosition> {
        if (leaves.isEmpty()) return emptyList()

        val subject = leaves.filterIsInstance<FactFilter.Predicate.Subject>().firstOrNull()
        val subjectPrefix = leaves.filterIsInstance<FactFilter.Predicate.SubjectPrefix>().firstOrNull()
        val type = leaves.filterIsInstance<FactFilter.Predicate.Type>().firstOrNull()
        val tags = leaves.filterIsInstance<FactFilter.Predicate.Tag>()
        val metadata = leaves.filterIsInstance<FactFilter.Predicate.Metadata>().firstOrNull()
        val timeRange = leaves.filterIsInstance<FactFilter.Predicate.TimeRange>().firstOrNull()

        // Residual = leaves not covered by the chosen primary index
        fun residualOf(vararg used: FactFilter.Predicate?): FactFilter.Predicate? {
            val usedSet = used.filterNotNull().toSet()
            val remaining = leaves.filter { it !in usedSet }
            return when (remaining.size) {
                0 -> null
                1 -> remaining[0]
                else -> FactFilter.Predicate.AllOf(remaining)
            }
        }

        return when {
            // Subject + Type → combined index (fastest for state-machine queries)
            subject != null && type != null -> {
                val range = ctx.subjectTypeIndexSubspace.range(storeId, subject.value, type.value)
                val afterKey = afterPosition?.let {
                    ctx.subjectTypeIndexSubspace.subspace.pack(
                        Tuple.from(storeId.uuid, subject.value.value, type.value.value, it)
                    )
                }
                val residual = residualOf(subject, type)
                scanIndex(range, afterKey, ctx.subjectTypeIndexSubspace::unpackPosition, residual, storeId, direction, hardLimit)
            }

            // Subject → subject index
            subject != null -> {
                val range = ctx.subjectIndexSubspace.range(storeId, subject.value)
                val afterKey = afterPosition?.let {
                    ctx.subjectIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, subject.value.value, it))
                }
                val residual = residualOf(subject)
                scanIndex(range, afterKey, ctx.subjectIndexSubspace::unpackPosition, residual, storeId, direction, hardLimit)
            }

            // Type + Tag → combined type-tag index
            type != null && tags.isNotEmpty() -> {
                val tag = tags.first()
                val range = ctx.tagsTypeIndexSubspace.range(storeId, type.value, tag.key to tag.value)
                val afterKey = afterPosition?.let {
                    ctx.tagsTypeIndexSubspace.subspace.pack(
                        Tuple.from(storeId.uuid, type.value.value, tag.key.value, tag.value.value, it)
                    )
                }
                val residual = residualOf(type, tag)
                scanIndex(range, afterKey, ctx.tagsTypeIndexSubspace::unpackPosition, residual, storeId, direction, hardLimit)
            }

            // Tag(s) only → tag index; multiple tags use intersection
            tags.isNotEmpty() -> {
                val primaryTag = tags.first()
                val range = ctx.tagsIndexSubspace.range(storeId, primaryTag.key, primaryTag.value)
                val afterKey = afterPosition?.let {
                    ctx.tagsIndexSubspace.subspace.pack(
                        Tuple.from(storeId.uuid, primaryTag.key.value, primaryTag.value.value, it)
                    )
                }
                val residual = residualOf(primaryTag)
                // A hard limit is only safe on the primary branch when there is exactly one
                // tag — with more than one, the branches are intersected below, and the
                // top-n of each branch individually does not guarantee the top-n of the
                // intersection.
                val primaryHardLimit = if (tags.size == 1) hardLimit else null
                val primaryPositions =
                    scanIndex(range, afterKey, ctx.tagsIndexSubspace::unpackPosition, residual, storeId, direction, primaryHardLimit)

                if (tags.size == 1) primaryPositions
                else {
                    // Intersect with remaining tag indexes
                    val remainingTags = tags.drop(1)
                    val otherSets = coroutineScope {
                        remainingTags.map { tag ->
                            async {
                                val r = ctx.tagsIndexSubspace.range(storeId, tag.key, tag.value)
                                val ak = afterPosition?.let {
                                    ctx.tagsIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, tag.key.value, tag.value.value, it))
                                }
                                scanIndex(r, ak, ctx.tagsIndexSubspace::unpackPosition, null, storeId, direction).toHashSet()
                            }
                        }.map { it.await() }
                    }
                    val primarySet = primaryPositions.toHashSet()
                    otherSets.fold(primarySet) { acc, s -> (acc intersect s).toHashSet() }.toList()
                }
            }

            // Type only → event-type index
            type != null -> {
                val range = ctx.eventTypeIndexSubspace.range(storeId, type.value)
                val afterKey = afterPosition?.let {
                    ctx.eventTypeIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, type.value.value, it))
                }
                val residual = residualOf(type)
                scanIndex(range, afterKey, ctx.eventTypeIndexSubspace::unpackPosition, residual, storeId, direction, hardLimit)
            }

            // Metadata → metadata index
            metadata != null -> {
                val range = ctx.metadataIndexSubspace.range(storeId, metadata.key, metadata.value)
                val afterKey = afterPosition?.let {
                    ctx.metadataIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, metadata.key, metadata.value, it))
                }
                val residual = residualOf(metadata)
                scanIndex(range, afterKey, ctx.metadataIndexSubspace::unpackPosition, residual, storeId, direction, hardLimit)
            }

            // SubjectPrefix → prefix range scan on subject index; the prefix spans many
            // distinct subject values, so a single afterKey can't be constructed the way
            // it can for an exact subject match — afterPosition is instead filtered in-app.
            subjectPrefix != null -> {
                val range = ctx.subjectIndexSubspace.prefixRange(storeId, subjectPrefix.prefix)
                val residual = residualOf(subjectPrefix)
                // A hard limit is only safe when there's no post-hoc afterPosition filter
                // to follow — that filter can drop rows from within the capped raw scan.
                val effectiveHardLimit = if (afterPosition == null) hardLimit else null
                scanIndex(range, afterKey = null, ctx.subjectIndexSubspace::unpackPosition, residual, storeId, direction, effectiveHardLimit)
                    .let { positions ->
                        if (afterPosition == null) positions
                        else if (direction == ReadDirection.Backward) positions.filter { it < afterPosition }
                        else positions.filter { it > afterPosition }
                    }
            }

            // TimeRange → createdAt index; filter afterPosition in-app (key format mixes time + vs)
            timeRange != null -> {
                val indexRange = ctx.createdAtIndexSubspace.range(storeId)
                val begin = timeRange.range.start?.let { ctx.createdAtIndexSubspace.getKey(storeId, it) }
                    ?: indexRange.begin
                val end = timeRange.range.end?.let { ctx.createdAtIndexSubspace.getKey(storeId, it) }
                    ?: indexRange.end
                val residual = residualOf(timeRange)
                val effectiveHardLimit = if (afterPosition == null) hardLimit else null
                scanIndex(Range(begin, end), afterKey = null, ctx.createdAtIndexSubspace::unpackPosition, residual, storeId, direction, effectiveHardLimit)
                    .let { positions ->
                        if (afterPosition == null) positions
                        else if (direction == ReadDirection.Backward) positions.filter { it < afterPosition }
                        else positions.filter { it > afterPosition }
                    }
            }

            else -> emptyList()
        }
    }

    /**
     * Scans a single index range, optionally starting after [afterKey], and returns
     * matching [FactPosition]s. If [residual] is present, loads each fact and applies
     * the predicate in-application.
     *
     * [direction] determines which side of [afterKey] is scanned: forward returns
     * positions after [afterKey] (or from the start of [range]); backward returns
     * positions before [afterKey] (or from the end of [range]), scanned in descending
     * order. [rowLimit], when given, is pushed down as a native FDB row limit — the
     * caller (see [collectLeafPositions]) only supplies one when doing so cannot
     * silently under-return, i.e. when there is no [residual] and no separate
     * in-application cursor filter following this call.
     */
    private suspend fun scanIndex(
        range: Range,
        afterKey: ByteArray?,
        unpackPosition: (ByteArray) -> FactPosition,
        residual: FactFilter.Predicate?,
        storeId: StoreId,
        direction: ReadDirection = ReadDirection.Forward,
        rowLimit: Int? = null,
    ): List<FactPosition> = read { tr ->
        val reverse = direction == ReadDirection.Backward
        val begin = if (!reverse && afterKey != null) KeySelector.firstGreaterThan(afterKey)
                    else KeySelector.firstGreaterOrEqual(range.begin)
        val end = if (reverse && afterKey != null) KeySelector.firstGreaterOrEqual(afterKey)
                  else KeySelector.firstGreaterOrEqual(range.end)

        val effectiveLimit = if (residual == null) (rowLimit ?: ReadTransaction.ROW_LIMIT_UNLIMITED)
                              else ReadTransaction.ROW_LIMIT_UNLIMITED

        tr.snapshot().getRange(begin, end, effectiveLimit, reverse).asList()
            .thenCompose { kvs ->
                val positions = kvs.map { unpackPosition(it.key) }

                if (residual == null) {
                    CompletableFuture.completedFuture(positions)
                } else {
                    val loadFutures = positions.map { pos ->
                        with(tr) { with(store) { storeId.run { pos.loadFactByPosition() } } }
                    }
                    CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                        positions.zip(loadFutures).mapNotNull { (pos, future) ->
                            val fact = future.resultNow()?.fact ?: return@mapNotNull null
                            if (residual.matches(fact)) pos else null
                        }
                    }
                }
            }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private suspend fun <T> read(block: (ReadTransaction) -> CompletableFuture<T>): T =
        store.db.readAsync(block).await()
}

private fun FactFilter.Predicate.isLeafPredicate(): Boolean = when (this) {
    is FactFilter.Predicate.Subject,
    is FactFilter.Predicate.SubjectPrefix,
    is FactFilter.Predicate.Type,
    is FactFilter.Predicate.Tag,
    is FactFilter.Predicate.Metadata,
    is FactFilter.Predicate.TimeRange -> true
    else -> false
}
