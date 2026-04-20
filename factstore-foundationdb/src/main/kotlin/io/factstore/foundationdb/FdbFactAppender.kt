package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import io.factstore.core.*
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

const val REVERSED = true
const val LIMIT_ONE = 1
const val OR_EQUAL = true
const val ZERO_OFFSET = 0

class FdbFactAppender(
    private val store: FdbFactStore,
) : FactAppender {

    override suspend fun append(factStoreId: FactStoreId, fact: Fact): AppendResult =
        append(factStoreId, listOf(fact))

    override suspend fun append(factStoreId: FactStoreId, facts: List<Fact>): AppendResult =
        append(
            AppendRequest(
                factStoreId = factStoreId,
                facts = facts,
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        )

    override suspend fun append(request: AppendRequest): AppendResult =
        store.db.runAsync { tr ->

            // check fact store exists
            store.context.getMetadata(request.factStoreId, tr).thenCompose { metadata ->
                if (metadata == null) {
                    return@thenCompose CompletableFuture.completedFuture(AppendResult.FactStoreNotFound)
                } else {
                    val key = request.idempotencyKeyBytes()

                    tr.get(key).thenCompose { existing ->
                        if (existing != null) {
                            CompletableFuture.completedFuture(AppendResult.AlreadyApplied)
                        } else {
                            with(tr) {
                                request.validate().thenCompose {
                                    request.appendNew(key)
                                }
                            }
                        }
                    }
                }
            }
        }.await()

    context(transaction: Transaction)
    private fun AppendRequest.validate(): CompletableFuture<Unit> {
        val checks: List<CompletableFuture<FactId?>> = facts.map { fact ->
            store.context.factPositionIndexSubspace.exists(factStoreId, fact.id).thenApply { exists ->
                if (exists) fact.id else null
            }
        }

        return CompletableFuture
            .allOf(*checks.toTypedArray())
            .thenApply {
                val duplicatedFactIds = checks.mapNotNull { it.resultNow() }
                if (duplicatedFactIds.isNotEmpty()) {
                    throw DuplicateFactIdException(duplicatedFactIds)
                }
            }
    }

    context(tr: Transaction)
    private fun AppendRequest.appendNew(
        idempotencyKeyBytes: ByteArray
    ): CompletableFuture<AppendResult> {

        return this.condition.isSatisfied().thenApply { satisfied ->
            if (!satisfied) {
                AppendResult.AppendConditionViolated
            } else {
                this.facts.store()
                tr.set(idempotencyKeyBytes, EMPTY_BYTE_ARRAY)
                AppendResult.Appended
            }
        }
    }

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun AppendCondition.isSatisfied(): CompletableFuture<Boolean> =
        when (this) {
            AppendCondition.None -> CompletableFuture.completedFuture(true)
            is AppendCondition.ExpectedLastFact -> isSatisfied()
            is AppendCondition.TagQueryBased -> isSatisfied()
            is AppendCondition.ExpectedMultiSubjectLastFact -> isSatisfied()
        }

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun AppendCondition.ExpectedLastFact.isSatisfied(): CompletableFuture<Boolean> {
        val actualLastFactId = subjectRef.getLastFactId()
        val isConditionSatisfied = actualLastFactId == expectedLastFactId
        return CompletableFuture.completedFuture(isConditionSatisfied)
    }

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun AppendCondition.ExpectedMultiSubjectLastFact.isSatisfied(): CompletableFuture<Boolean> {
        val isSatisfied = expectations.all { it.key.getLastFactId() == it.value }
        return CompletableFuture.completedFuture(isSatisfied)
    }

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun SubjectRef.getLastFactId(): FactId? {
        val subjectRange = store.context.subjectIndexSubspace.range(appendRequest.factStoreId, this)
        val latestFactKeyValue = tr.getRange(subjectRange, LIMIT_ONE, REVERSED).firstOrNull()
        return latestFactKeyValue?.let {
            Tuple.fromBytes(it.value).getFirstAsFactId()
        }
    }

    private fun AppendRequest.idempotencyKeyBytes(): ByteArray =
        store.context.idempotencySubspace.pack(Tuple.from(idempotencyKey.value))

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun List<Fact>.store() = with(store) {
        appendRequest.factStoreId.apply { this@store.store() }
    }

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun AppendCondition.TagQueryBased.isSatisfied(): CompletableFuture<Boolean> {
        return after?.getPosition()?.thenCompose { position ->
            queryItemsForPosition(position)
        } ?: queryItemsForPosition()
    }

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun FactId.getPosition() = with(store) {
        context.factPositionIndexSubspace.getPosition(appendRequest.factStoreId, this@getPosition)
    }

    context(tr: Transaction, appendRequest: AppendRequest)
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

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun TagQueryItem.resolveFactIds(
        afterPosition: FactPosition? = null
    ): CompletableFuture<Set<FactId>> = when (this) {
        is TagOnlyQueryItem -> queryByTags(afterPosition)
        is TagTypeItem -> queryByTypeAndTags(afterPosition)
    }

    context(tr: Transaction, appendRequest: AppendRequest)
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
                store.context.tagsIndexSubspace.getKey(appendRequest.factStoreId, tag, afterPosition)
            } else {
                // If there's no afterPosition, just use the tag
                store.context.tagsIndexSubspace.getKey(appendRequest.factStoreId, tag)
            }

            // Create the beginSelector (first greater than if afterPosition is provided)
            val beginSelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(key)
            } else {
                KeySelector(key, OR_EQUAL, ZERO_OFFSET)
            }

            // Create the end selector based on the tag range
            val range = store.context.tagsIndexSubspace.range(appendRequest.factStoreId, tag)
            val endSelector = KeySelector.lastLessOrEqual(range.end)

            return Pair(beginSelector, endSelector)
        }

        val futures: List<CompletableFuture<Set<FactId>>> = tags.map { tag ->
            val (beginSelector, endSelector) = createSelectors(tag, afterPosition)

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

    context(tr: Transaction, appendRequest: AppendRequest)
    private fun TagTypeItem.queryByTypeAndTags(
        afterPosition: FactPosition?
    ): CompletableFuture<Set<FactId>> {
        // Helper function to create start and end selectors
        fun createSelectors(
            type: FactType,
            tag: Pair<TagKey, TagValue>,
            afterPosition: FactPosition?
        ): Pair<KeySelector, KeySelector> {
            val tuple = if (afterPosition != null) {
                Tuple.from(appendRequest.factStoreId.uuid, type.value, tag.first.value, tag.second.value, afterPosition)
            } else {
                Tuple.from(appendRequest.factStoreId.uuid, type.value, tag.first.value, tag.second.value)
            }

            val startKeySelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(store.context.tagsTypeIndexSubspace.pack(tuple))
            } else {
                KeySelector(store.context.tagsTypeIndexSubspace.pack(tuple), OR_EQUAL, ZERO_OFFSET)
            }

            val range = store.context.tagsTypeIndexSubspace.subspace(Tuple.from(appendRequest.factStoreId.uuid, type.value, tag.first.value, tag.second.value)).range()
            val endSelector = KeySelector.lastLessOrEqual(range.end)

            return Pair(startKeySelector, endSelector)
        }

        // use composite "type+tag" index
        val futures: List<CompletableFuture<Set<FactId>>> = types.map { type ->
            val tagFutures = tags.map { tag ->
                // Create the start and end selectors
                val (startKeySelector, endSelector) = createSelectors(type, tag, afterPosition)

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
