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

        val stream = buildStream(storeId, query.filter, afterPosition, query.direction, query.limit)
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
                    val begin = if (cursor == null) {
                        if (isReversed) KeySelector.firstGreaterOrEqual(globalRange.end)
                        else KeySelector.firstGreaterOrEqual(globalRange.begin)
                    } else {
                        if (isReversed) KeySelector.lastLessThan(cursor!!)
                        else KeySelector.firstGreaterThan(cursor!!)
                    }
                    val end = if (isReversed)
                        KeySelector.firstGreaterOrEqual(globalRange.begin)
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
    ): List<FactPosition> = when (predicate) {

        is FactFilter.Predicate.AnyOf -> {
            coroutineScope {
                predicate.predicates
                    .map { child -> async { collectPositions(storeId, child, afterPosition, direction, outerLeaves) } }
                    .flatMap { it.await() }
            }
        }

        is FactFilter.Predicate.AllOf -> {
            val myLeaves = predicate.predicates.filter { it.isLeafPredicate() }
            val combined = outerLeaves + myLeaves
            val nonLeaves = predicate.predicates.filter { !it.isLeafPredicate() }

            when {
                nonLeaves.isEmpty() ->
                    collectLeafPositions(storeId, combined, afterPosition)

                nonLeaves.size == 1 ->
                    collectPositions(storeId, nonLeaves[0], afterPosition, direction, combined)

                else -> {
                    // Multiple non-leaf children → AND semantics → intersect position sets
                    val sets = coroutineScope {
                        nonLeaves.map { child ->
                            async { collectPositions(storeId, child, afterPosition, direction, combined).toHashSet() }
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
            collectLeafPositions(storeId, outerLeaves + predicate, afterPosition)
    }

    /**
     * Collects positions for a bounded selector ([FactFilter.Predicate.Last]/[FactFilter.Predicate.First]).
     * Scans the inner predicate's index in [boundDirection], taking up to [n] matches.
     */
    private suspend fun collectBoundedPositions(
        storeId: StoreId,
        n: Int,
        predicate: FactFilter.Predicate,
        boundDirection: ReadDirection,
        afterPosition: FactPosition?,
    ): List<FactPosition> =
        collectPositions(storeId, predicate, afterPosition, boundDirection)
            .toSortedSet(boundDirection.toComparator())
            .take(n)

    /**
     * Collects positions for a set of leaf-predicate constraints. Selects the
     * most efficient FDB index based on predicate types, then applies any
     * remaining predicates as an in-application residual filter.
     */
    private suspend fun collectLeafPositions(
        storeId: StoreId,
        leaves: List<FactFilter.Predicate>,
        afterPosition: FactPosition?,
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
                scanIndex(range, afterKey, ctx.subjectTypeIndexSubspace::unpackPosition, residualOf(subject, type), storeId)
            }

            // Subject → subject index
            subject != null -> {
                val range = ctx.subjectIndexSubspace.range(storeId, subject.value)
                val afterKey = afterPosition?.let {
                    ctx.subjectIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, subject.value.value, it))
                }
                scanIndex(range, afterKey, ctx.subjectIndexSubspace::unpackPosition, residualOf(subject), storeId)
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
                scanIndex(range, afterKey, ctx.tagsTypeIndexSubspace::unpackPosition, residualOf(type, tag), storeId)
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
                val primaryPositions = scanIndex(range, afterKey, ctx.tagsIndexSubspace::unpackPosition, residual, storeId)

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
                                scanIndex(r, ak, ctx.tagsIndexSubspace::unpackPosition, null, storeId).toHashSet()
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
                scanIndex(range, afterKey, ctx.eventTypeIndexSubspace::unpackPosition, residualOf(type), storeId)
            }

            // Metadata → metadata index
            metadata != null -> {
                val range = ctx.metadataIndexSubspace.range(storeId, metadata.key, metadata.value)
                val afterKey = afterPosition?.let {
                    ctx.metadataIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, metadata.key, metadata.value, it))
                }
                scanIndex(range, afterKey, ctx.metadataIndexSubspace::unpackPosition, residualOf(metadata), storeId)
            }

            // SubjectPrefix → prefix range scan on subject index, filter afterPosition in-app
            subjectPrefix != null -> {
                val range = ctx.subjectIndexSubspace.subspace.range(Tuple.from(storeId.uuid, subjectPrefix.prefix))
                val residual = residualOf(subjectPrefix)
                scanIndex(range, afterKey = null, ctx.subjectIndexSubspace::unpackPosition, residual, storeId)
                    .let { positions ->
                        if (afterPosition == null) positions
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
                scanIndex(Range(begin, end), afterKey = null, ctx.createdAtIndexSubspace::unpackPosition, residual, storeId)
                    .let { positions ->
                        if (afterPosition == null) positions
                        else positions.filter { it > afterPosition }
                    }
            }

            else -> emptyList()
        }
    }

    /**
     * Scans a single index range, optionally starting after [afterKey], and returns
     * all matching [FactPosition]s. If [residual] is present, loads each fact and
     * applies the predicate in-application.
     */
    private suspend fun scanIndex(
        range: Range,
        afterKey: ByteArray?,
        unpackPosition: (ByteArray) -> FactPosition,
        residual: FactFilter.Predicate?,
        storeId: StoreId,
    ): List<FactPosition> = read { tr ->
        val begin = if (afterKey == null) KeySelector.firstGreaterOrEqual(range.begin)
                    else KeySelector.firstGreaterThan(afterKey)
        val end = KeySelector.firstGreaterOrEqual(range.end)

        tr.snapshot().getRange(begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED, false).asList()
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
