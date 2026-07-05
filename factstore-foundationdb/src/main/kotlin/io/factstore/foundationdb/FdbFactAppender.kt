package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import io.factstore.core.FactFilter
import io.factstore.core.matches
import io.factstore.core.*
import kotlinx.coroutines.future.await
import java.time.Instant
import java.util.concurrent.CompletableFuture

const val REVERSED = true
const val LIMIT_ONE = 1
const val OR_EQUAL = true
const val ZERO_OFFSET = 0

class FdbFactAppender(
    private val store: FdbFactStore,
) : FactAppender {

    override suspend fun append(storeName: StoreName, fact: FactInput): AppendResult =
        append(storeName, listOf(fact))

    override suspend fun append(storeName: StoreName, facts: List<FactInput>): AppendResult =
        append(
            AppendRequest(
                storeName = storeName,
                facts = facts,
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        )

    override suspend fun append(request: AppendRequest): AppendResult =
        store.db.runAsync { tr ->
            with(tr) {
                // check fact store exists
                store.context.lookUpStoreIdByName(request.storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(AppendResult.StoreNotFound(request.storeName))
                    } else {
                        appendToStore(storeId, request)
                    }
                }
            }
        }.await()

    context(tr: Transaction)
    private fun appendToStore(
        storeId: StoreId,
        request: AppendRequest,
    ): CompletableFuture<AppendResult> = with(storeId) {
        val idempotencyKey = request.idempotencyKeyBytes()

        tr[idempotencyKey].thenCompose { existing ->
            if (existing != null) {
                CompletableFuture.completedFuture(AppendResult.AlreadyApplied)
            } else {
                with(storeId) {
                    val appendedAt = Instant.now()
                    val facts = request.facts.map { it.toFact(FactId.generate(), appendedAt) }
                    request.appendNew(facts, appendedAt)
                }
            }
        }
    }

    context(tr: Transaction, storeId: StoreId)
    private fun AppendRequest.appendNew(
        facts: List<Fact>,
        appendedAt: Instant,
    ): CompletableFuture<AppendResult> =
        condition.isSatisfied().thenApply { satisfied ->
            if (!satisfied) {
                AppendResult.AppendConditionViolated
            } else {
                facts.store()
                store.context.idempotencySubspace.save(storeId, idempotencyKey)
                AppendResult.Appended(facts.map { it.id }, appendedAt)
            }
        }

    context(tr: Transaction, storeId: StoreId)
    private fun AppendCondition.isSatisfied(): CompletableFuture<Boolean> =
        when (this) {
            AppendCondition.None -> CompletableFuture.completedFuture(true)
            is AppendCondition.ExpectedLastFact -> isSatisfied()
            is AppendCondition.TagQueryBased -> isSatisfied()
            is AppendCondition.All -> isSatisfied()
            is AppendCondition.IfNoneMatch -> isSatisfied()
        }

    context(tr: Transaction, storeId: StoreId)
    private fun AppendCondition.ExpectedLastFact.isSatisfied(): CompletableFuture<Boolean> {
        val actualLastFactId = subject.getLastFactId()
        val isConditionSatisfied = actualLastFactId == expectedLastFactId
        return CompletableFuture.completedFuture(isConditionSatisfied)
    }

    context(tr: Transaction, storeId: StoreId)
    private fun AppendCondition.All.isSatisfied(): CompletableFuture<Boolean> {
        val futures = conditions.map { it.isSatisfied() }
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
            futures.all { it.resultNow() }
        }
    }

    context(tr: Transaction, storeId: StoreId)
    private fun Subject.getLastFactId(): FactId? {
        val subjectRange = store.context.subjectIndexSubspace.range(storeId, this)
        val latestFactKeyValue = tr.getRange(subjectRange, LIMIT_ONE, REVERSED).firstOrNull()
        return latestFactKeyValue?.let {
            Tuple.fromBytes(it.value).getFirstAsFactId()
        }
    }

    context(storeId: StoreId)
    private fun AppendRequest.idempotencyKeyBytes(): ByteArray =
        store.context.idempotencySubspace.pack(storeId, idempotencyKey)

    context(tr: Transaction, appendRequest: AppendRequest, storeId: StoreId)
    private fun List<Fact>.store() = with(store) {
        appendRequest.storeName.apply { this@store.store() }
    }

    // ── IfNoneMatch condition ──────────────────────────────────────────────────

    context(tr: Transaction, storeId: StoreId)
    private fun AppendCondition.IfNoneMatch.isSatisfied(): CompletableFuture<Boolean> =
        (after?.let { store.context.factPositionIndexSubspace.getPosition(storeId, it) }
            ?: CompletableFuture.completedFuture(null))
            .thenCompose { afterPosition ->
                filter.anyMatchAfter(afterPosition).thenApply { hasMatch -> !hasMatch }
            }

    /**
     * Returns `true` if at least one fact matching this predicate exists after
     * [afterPosition]. Evaluated within the current write transaction so the
     * check is atomic with the append.
     *
     * Supports: leaf predicates (each backed by an FDB index), [FactFilter.Predicate.AnyOf]
     * (parallel fan-out), [FactFilter.Predicate.AllOf] (best-index scan + in-app filter),
     * and [FactFilter.Predicate.Last]/[FactFilter.Predicate.First] (delegates to inner predicate).
     */
    context(tr: Transaction, storeId: StoreId)
    private fun FactFilter.Predicate.anyMatchAfter(
        afterPosition: FactPosition?,
    ): CompletableFuture<Boolean> = when (this) {

        is FactFilter.Predicate.AnyOf -> {
            val futures = predicates.map { it.anyMatchAfter(afterPosition) }
            CompletableFuture.allOf(*futures.toTypedArray())
                .thenApply { futures.any { f -> f.resultNow() } }
        }

        is FactFilter.Predicate.AllOf -> allOfAnyMatchAfter(afterPosition)

        is FactFilter.Predicate.Last -> predicate.anyMatchAfter(afterPosition)
        is FactFilter.Predicate.First -> predicate.anyMatchAfter(afterPosition)

        is FactFilter.Predicate.Subject ->
            indexExistsAfter(
                range = store.context.subjectIndexSubspace.range(storeId, value),
                afterKey = afterPosition?.let {
                    store.context.subjectIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, value.value, it))
                }
            )

        is FactFilter.Predicate.Type ->
            indexExistsAfter(
                range = store.context.eventTypeIndexSubspace.range(storeId, value),
                afterKey = afterPosition?.let {
                    store.context.eventTypeIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, value.value, it))
                }
            )

        is FactFilter.Predicate.Tag ->
            indexExistsAfter(
                range = store.context.tagsIndexSubspace.range(storeId, key, value),
                afterKey = afterPosition?.let {
                    store.context.tagsIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, key.value, value.value, it))
                }
            )

        is FactFilter.Predicate.Metadata ->
            indexExistsAfter(
                range = store.context.metadataIndexSubspace.range(storeId, key, value),
                afterKey = afterPosition?.let {
                    store.context.metadataIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, key, value, it))
                }
            )

        is FactFilter.Predicate.SubjectPrefix -> {
            val range = store.context.subjectIndexSubspace.subspace.range(Tuple.from(storeId.uuid, prefix))
            if (afterPosition == null) {
                indexExistsAfter(range, afterKey = null)
            } else {
                // Scan all prefix entries; filter by position > afterPosition in-app
                tr.snapshot().getRange(
                    KeySelector.firstGreaterOrEqual(range.begin),
                    KeySelector.firstGreaterOrEqual(range.end)
                ).asList().thenApply { kvs ->
                    kvs.any { kv -> store.context.subjectIndexSubspace.unpackPosition(kv.key) > afterPosition }
                }
            }
        }

        is FactFilter.Predicate.TimeRange -> {
            val indexRange = store.context.createdAtIndexSubspace.range(storeId)
            val begin = range.start?.let { store.context.createdAtIndexSubspace.getKey(storeId, it) }
                ?: indexRange.begin
            val end = range.end?.let { store.context.createdAtIndexSubspace.getKey(storeId, it) }
                ?: indexRange.end
            if (afterPosition == null) {
                indexExistsAfter(Range(begin, end), afterKey = null)
            } else {
                tr.snapshot().getRange(
                    KeySelector.firstGreaterOrEqual(begin),
                    KeySelector.firstGreaterOrEqual(end)
                ).asList().thenApply { kvs ->
                    kvs.any { kv -> store.context.createdAtIndexSubspace.unpackPosition(kv.key) > afterPosition }
                }
            }
        }
    }

    /**
     * Handles [FactFilter.Predicate.AllOf] by selecting the best single index
     * and applying any remaining predicates as in-application filters on loaded facts.
     */
    context(tr: Transaction, storeId: StoreId)
    private fun FactFilter.Predicate.AllOf.allOfAnyMatchAfter(
        afterPosition: FactPosition?,
    ): CompletableFuture<Boolean> {
        val subject = predicates.filterIsInstance<FactFilter.Predicate.Subject>().firstOrNull()
        val type = predicates.filterIsInstance<FactFilter.Predicate.Type>().firstOrNull()
        val tags = predicates.filterIsInstance<FactFilter.Predicate.Tag>()
        val metadata = predicates.filterIsInstance<FactFilter.Predicate.Metadata>().firstOrNull()

        // Select primary index and build residual predicate for in-app filtering
        data class PrimaryIndex(
            val range: com.apple.foundationdb.Range,
            val afterKey: ByteArray?,
            val unpack: (ByteArray) -> FactPosition,
            val residual: FactFilter.Predicate?,
        )

        fun residualOf(vararg used: FactFilter.Predicate?): FactFilter.Predicate? {
            val usedSet = used.filterNotNull().toSet()
            val remaining = predicates.filter { it !in usedSet }
            return when (remaining.size) {
                0 -> null; 1 -> remaining[0]; else -> FactFilter.Predicate.AllOf(remaining)
            }
        }

        val primary: PrimaryIndex? = when {
            subject != null && type != null -> PrimaryIndex(
                range = store.context.subjectTypeIndexSubspace.range(storeId, subject.value, type.value),
                afterKey = afterPosition?.let {
                    store.context.subjectTypeIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, subject.value.value, type.value.value, it))
                },
                unpack = store.context.subjectTypeIndexSubspace::unpackPosition,
                residual = residualOf(subject, type),
            )
            subject != null -> PrimaryIndex(
                range = store.context.subjectIndexSubspace.range(storeId, subject.value),
                afterKey = afterPosition?.let {
                    store.context.subjectIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, subject.value.value, it))
                },
                unpack = store.context.subjectIndexSubspace::unpackPosition,
                residual = residualOf(subject),
            )
            type != null && tags.isNotEmpty() -> {
                val tag = tags.first()
                PrimaryIndex(
                    range = store.context.tagsTypeIndexSubspace.range(storeId, type.value, tag.key to tag.value),
                    afterKey = afterPosition?.let {
                        store.context.tagsTypeIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, type.value.value, tag.key.value, tag.value.value, it))
                    },
                    unpack = store.context.tagsTypeIndexSubspace::unpackPosition,
                    residual = residualOf(type, tag),
                )
            }
            tags.isNotEmpty() -> {
                val tag = tags.first()
                PrimaryIndex(
                    range = store.context.tagsIndexSubspace.range(storeId, tag.key, tag.value),
                    afterKey = afterPosition?.let {
                        store.context.tagsIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, tag.key.value, tag.value.value, it))
                    },
                    unpack = store.context.tagsIndexSubspace::unpackPosition,
                    residual = residualOf(tag),
                )
            }
            type != null -> PrimaryIndex(
                range = store.context.eventTypeIndexSubspace.range(storeId, type.value),
                afterKey = afterPosition?.let {
                    store.context.eventTypeIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, type.value.value, it))
                },
                unpack = store.context.eventTypeIndexSubspace::unpackPosition,
                residual = residualOf(type),
            )
            metadata != null -> PrimaryIndex(
                range = store.context.metadataIndexSubspace.range(storeId, metadata.key, metadata.value),
                afterKey = afterPosition?.let {
                    store.context.metadataIndexSubspace.subspace.pack(Tuple.from(storeId.uuid, metadata.key, metadata.value, it))
                },
                unpack = store.context.metadataIndexSubspace::unpackPosition,
                residual = residualOf(metadata),
            )
            else -> null
        }

        if (primary == null) return CompletableFuture.completedFuture(false)

        if (primary.residual == null) {
            return indexExistsAfter(primary.range, primary.afterKey)
        }

        // Residual filter: load facts and check in-app
        val fullPredicate = this
        val begin = if (primary.afterKey == null) KeySelector.firstGreaterOrEqual(primary.range.begin)
                    else KeySelector.firstGreaterThan(primary.afterKey)
        val end = KeySelector.firstGreaterOrEqual(primary.range.end)

        return tr.snapshot().getRange(begin, end).asList().thenCompose { kvs ->
            if (kvs.isEmpty()) return@thenCompose CompletableFuture.completedFuture(false)
            val positions = kvs.map { primary.unpack(it.key) }
            val loadFutures = positions.map { pos ->
                with(tr) { with(store) { storeId.run { pos.loadFactByPosition() } } }
            }
            CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                loadFutures.any { f -> f.resultNow()?.fact?.let { fullPredicate.matches(it) } == true }
            }
        }
    }

    /**
     * Returns `true` if any key exists in [range] after [afterKey] (exclusive).
     * Uses [LIMIT_ONE] for efficiency — only existence is checked.
     */
    context(tr: Transaction)
    private fun indexExistsAfter(range: com.apple.foundationdb.Range, afterKey: ByteArray?): CompletableFuture<Boolean> {
        val begin = if (afterKey == null) KeySelector.firstGreaterOrEqual(range.begin)
                    else KeySelector.firstGreaterThan(afterKey)
        val end = KeySelector.firstGreaterOrEqual(range.end)
        return tr.snapshot().getRange(begin, end, LIMIT_ONE).asList().thenApply { it.isNotEmpty() }
    }

    context(tr: Transaction, storeId: StoreId)
    private fun AppendCondition.TagQueryBased.isSatisfied(): CompletableFuture<Boolean> {
        return after?.getPosition()?.thenCompose { position ->
            queryItemsForPosition(position)
        } ?: queryItemsForPosition()
    }

    context(tr: Transaction, storeId: StoreId)
    private fun FactId.getPosition() = with(store) {
        context.factPositionIndexSubspace.getPosition(storeId, this@getPosition)
    }

    context(tr: Transaction, storeId: StoreId)
    private fun AppendCondition.TagQueryBased.queryItemsForPosition(
        afterPosition: FactPosition? = null
    ): CompletableFuture<Boolean> {
        val queryItemFutures = failIfEventsMatch.queryItems.map { queryItem ->
            queryItem.resolveFactIds(afterPosition)
        }

        return CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenApply {
            val factIds = queryItemFutures
                .flatMap { it.getNow(emptySet()) }
                .toSet()  // OR semantics = union

            factIds.isEmpty()
        }
    }

    context(tr: Transaction, storeId: StoreId)
    private fun TagQueryItem.resolveFactIds(
        afterPosition: FactPosition? = null
    ): CompletableFuture<Set<FactId>> = when (this) {
        is TagOnlyQueryItem -> queryByTags(afterPosition)
        is TagTypeItem -> queryByTypeAndTags(afterPosition)
    }

    context(tr: Transaction, storeId: StoreId)
    private fun TagOnlyQueryItem.queryByTags(
        afterPosition: FactPosition?
    ): CompletableFuture<Set<FactId>> {

        // Helper function to create begin and end selectors for the range query
        fun createSelectors(
            tag: Pair<TagKey, TagValue>,
            afterPosition: FactPosition?
        ): Pair<KeySelector, KeySelector> {
            val key = if (afterPosition != null) {
                // If there's a afterPosition, include it in the tuple
                store.context.tagsIndexSubspace.getKey(storeId, tag, afterPosition)
            } else {
                // If there's no afterPosition, just use the tag
                store.context.tagsIndexSubspace.getKey(storeId, tag)
            }

            // Create the beginSelector (first greater than if afterPosition is provided)
            val beginSelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(key)
            } else {
                KeySelector(key, OR_EQUAL, ZERO_OFFSET)
            }

            // Create the end selector based on the tag range
            val range = store.context.tagsIndexSubspace.range(storeId, tag)
            val endSelector = KeySelector.lastLessOrEqual(range.end)

            return Pair(beginSelector, endSelector)
        }

        val futures: List<CompletableFuture<Set<FactId>>> = tags.map { (key, value) ->
            val (beginSelector, endSelector) = createSelectors(key to value, afterPosition)

            tr.getRange(beginSelector, endSelector, LIMIT_ONE)
                .asList()
                .thenApply { keyValues ->
                    keyValues.map {
                        Tuple.fromBytes(it.value).getFirstAsFactId()
                    }.toSet() // Convert to Set to easily combine results
                }
        }

        // After all futures complete, perform the union of the sets
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
            // Union the sets from all futures
            futures
                .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                .reduce { acc, set -> acc.union(set) } // Union all sets to get all matching fact IDs
                .orEmpty() // Return empty set if no sets are present
        }
    }

    context(tr: Transaction, storeId: StoreId)
    private fun TagTypeItem.queryByTypeAndTags(
        afterPosition: FactPosition?
    ): CompletableFuture<Set<FactId>> {
        // Helper function to create start and end selectors
        fun createSelectors(
            type: FactType,
            tag: Pair<TagKey, TagValue>,
            afterPosition: FactPosition?
        ): Pair<KeySelector, KeySelector> {
            val key = if (afterPosition != null) {
                store.context.tagsTypeIndexSubspace.getKey(storeId, type, tag, afterPosition)
            } else {
                store.context.tagsTypeIndexSubspace.getKey(storeId, type, tag)
            }

            val startKeySelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(key)
            } else {
                KeySelector(key, OR_EQUAL, ZERO_OFFSET)
            }

            val range = store.context.tagsTypeIndexSubspace.range(storeId, type, tag)
            val endSelector = KeySelector.lastLessOrEqual(range.end)

            return Pair(startKeySelector, endSelector)
        }

        // use composite "type+tag" index
        val futures: List<CompletableFuture<Set<FactId>>> = types.map { type ->
            val tagFutures = tags.map { (key, value) ->
                // Create the start and end selectors
                val (startKeySelector, endSelector) = createSelectors(type, key to value, afterPosition)

                tr.getRange(startKeySelector, endSelector, LIMIT_ONE)
                    .asList()
                    .thenApply { keyValues ->
                        keyValues.map {
                            Tuple.fromBytes(it.value).getFirstAsFactId()
                        }.toSet()
                    }
            }

            // we want to logically "AND" the result of the tag queries
            CompletableFuture.allOf(*tagFutures.toTypedArray()).thenApply {
                tagFutures
                    .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                    .reduce { acc, set -> acc.intersect(set) } // Reduce by intersecting each set
                    .orEmpty() // If there are no sets to intersect, return an empty set
            }
        }

        // we finally union the found UUIDs
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
            futures
                .map { it.getNow(emptySet()) }
                .reduce { acc, set -> acc.union(set) }
                .orEmpty()
        }
    }

}
