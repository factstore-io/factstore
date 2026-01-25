package org.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import kotlinx.coroutines.future.await
import org.factstore.core.*
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.collections.orEmpty

const val REVERSED = true
const val LIMIT_ONE = 1
const val OR_EQUAL = true
const val ZERO_OFFSET = 0

class FdbFactAppender(
    private val store: FdbFactStore,
) : FactAppender {

    private val db = store.db
    private val idempotencySubspace = store.idempotencySubspace
    private val tagsIndexSubspace = store.tagsIndexSubspace
    private val tagsTypeIndexSubspace = store.tagsTypeIndexSubspace

    override suspend fun append(fact: Fact) {
        append(
            AppendRequest(
                facts = listOf(fact),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        )
    }

    override suspend fun append(facts: List<Fact>) {
        append(
            AppendRequest(
                facts = facts,
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        )
    }

    override suspend fun append(request: AppendRequest): AppendResult =
        db.runAsync { tr ->
            val key = request.idempotencyKeyBytes()

            tr.get(key).thenCompose { existing ->
                if (existing != null) {
                    CompletableFuture.completedFuture(AppendResult.AlreadyApplied)
                } else {
                    request.validate(tr).thenCompose {
                        appendNew(request, tr, key)
                    }
                }
            }
        }.await()

    private fun AppendRequest.validate(transaction: Transaction): CompletableFuture<Unit> {
        val checks: List<CompletableFuture<FactId?>> = facts.map { fact ->
            val factKey = store.factsSubspace.pack(fact.id.toTuple())
            transaction.get(factKey).thenApply { existing ->
                if (existing != null) fact.id else null
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

    private fun appendNew(
        request: AppendRequest,
        tr: Transaction,
        idempotencyKeyBytes: ByteArray
    ): CompletableFuture<AppendResult> {

        return request.condition.isSatisfied(tr).thenApply { satisfied ->
            if (!satisfied) {
                AppendResult.AppendConditionViolated
            } else {
                request.facts.store(tr)
                tr.set(idempotencyKeyBytes, EMPTY_BYTE_ARRAY)
                AppendResult.Appended
            }
        }
    }

    private fun AppendCondition.isSatisfied(tr: Transaction): CompletableFuture<Boolean> =
        when (this) {
            AppendCondition.None -> CompletableFuture.completedFuture(true)
            is AppendCondition.ExpectedLastFact -> isSatisfied(tr)
            is AppendCondition.TagQueryBased -> isSatisfied(tr)
            is AppendCondition.ExpectedMultiSubjectLastFact -> isSatisfied(tr)
        }

    private fun AppendCondition.ExpectedLastFact.isSatisfied(tr: Transaction): CompletableFuture<Boolean> {
        val actualLastFactId = subjectRef.getLastFactId(tr)
        val isConditionSatisfied = actualLastFactId == expectedLastFactId
        return CompletableFuture.completedFuture(isConditionSatisfied)
    }

    private fun AppendCondition.ExpectedMultiSubjectLastFact.isSatisfied(tr: Transaction): CompletableFuture<Boolean> {
        val isSatisfied = expectations.all { it.key.getLastFactId(tr) == it.value }
        return CompletableFuture.completedFuture(isSatisfied)
    }

    private fun SubjectRef.getLastFactId(tr: Transaction): FactId? {
        val subjectIndexKeyBegin = Tuple.from(type, id)
        val subjectRange = store.subjectIndexSubspace.range(subjectIndexKeyBegin)
        val latestFactKeyValue = tr.getRange(subjectRange, LIMIT_ONE, REVERSED).firstOrNull()
        return latestFactKeyValue?.let {
            store.subjectIndexSubspace.unpack(it.key).getLastAsFactId()
        }
    }

    private fun AppendRequest.idempotencyKeyBytes(): ByteArray =
        idempotencySubspace.pack(Tuple.from(idempotencyKey.value))

    private fun List<Fact>.store(transaction: Transaction) = with(store) {
        this@store.store(transaction)
    }

    private fun AppendCondition.TagQueryBased.isSatisfied(tr: Transaction): CompletableFuture<Boolean> {
        return after?.getPosition(tr)?.thenCompose { position ->
            queryItemsForPosition(tr, position)
        } ?: queryItemsForPosition(tr)
    }

    private fun FactId.getPosition(transaction: ReadTransaction) = with(store) {
        this@getPosition.getPosition(transaction)
    }

    private fun AppendCondition.TagQueryBased.queryItemsForPosition(
        tr: Transaction,
        afterPosition: Pair<Versionstamp, Long>? = null
    ): CompletableFuture<Boolean> {
        val queryItemFutures = failIfEventsMatch.queryItems.map { queryItem ->
            queryItem.resolveFactIds(tr, afterPosition)
        }

        return CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenApply {
            val factIds = queryItemFutures
                .flatMap { it.getNow(emptySet()) }
                .toSet()  // OR semantics = union

            factIds.isEmpty()
        }
    }

    private fun TagQueryItem.resolveFactIds(
        tr: ReadTransaction,
        afterPosition: Pair<Versionstamp, Long>? = null
    ): CompletableFuture<Set<UUID>> = when (this) {
        is TagOnlyQueryItem -> queryByTags(tr, afterPosition)
        is TagTypeItem -> queryByTypeAndTags(tr, afterPosition)
    }

    private fun TagOnlyQueryItem.queryByTags(
        tr: ReadTransaction,
        afterPosition: Pair<Versionstamp, Long>?
    ): CompletableFuture<Set<UUID>> {

        // Helper function to create begin and end selectors for the range query
        fun createSelectors(
            tag: Pair<TagKey, TagValue>,
            afterPosition: Pair<Versionstamp, Long>?
        ): Pair<KeySelector, KeySelector> {
            val tuple = if (afterPosition != null) {
                // If there's a afterPosition, include it in the tuple
                Tuple.from(tag.first.value, tag.second.value, afterPosition.first, afterPosition.second)
            } else {
                // If there's no afterPosition, just use the tag
                Tuple.from(tag.first.value, tag.second.value)
            }

            // Create the beginSelector (first greater than if afterPosition is provided)
            val beginSelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(tagsIndexSubspace.pack(tuple))
            } else {
                KeySelector(tagsIndexSubspace.pack(tuple), OR_EQUAL, ZERO_OFFSET)
            }

            // Create the end selector based on the tag range
            val range = tagsIndexSubspace.range(Tuple.from(tag.first.value, tag.second.value))
            val endSelector = KeySelector.lastLessOrEqual(range.end)

            return Pair(beginSelector, endSelector)
        }

        val futures: List<CompletableFuture<Set<UUID>>> = tags.map { tag ->
            val (beginSelector, endSelector) = createSelectors(tag, afterPosition)

            tr.getRange(beginSelector, endSelector, LIMIT_ONE)
                .asList()
                .thenApply { keyValues ->
                    keyValues.map {
                        Tuple.fromBytes(it.key).getLastAsUuid()
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

    private fun TagTypeItem.queryByTypeAndTags(
        tr: ReadTransaction,
        afterPosition: Pair<Versionstamp, Long>?
    ): CompletableFuture<Set<UUID>> {
        // Helper function to create start and end selectors
        fun createSelectors(
            type: FactType,
            tag: Pair<TagKey, TagValue>,
            afterPosition: Pair<Versionstamp, Long>?
        ): Pair<KeySelector, KeySelector> {
            val tuple = if (afterPosition != null) {
                Tuple.from(type.value, tag.first.value, tag.second.value, afterPosition.first, afterPosition.second)
            } else {
                Tuple.from(type.value, tag.first.value, tag.second.value)
            }

            val startKeySelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(tagsTypeIndexSubspace.pack(tuple))
            } else {
                KeySelector(tagsTypeIndexSubspace.pack(tuple), OR_EQUAL, ZERO_OFFSET)
            }

            val range = tagsTypeIndexSubspace.subspace(Tuple.from(type.value, tag.first.value, tag.second.value)).range()
            val endSelector = KeySelector.lastLessOrEqual(range.end)

            return Pair(startKeySelector, endSelector)
        }

        // use composite "type+tag" index
        val futures: List<CompletableFuture<Set<UUID>>> = types.map { type ->
            val tagFutures = tags.map { tag ->
                // Create the start and end selectors
                val (startKeySelector, endSelector) = createSelectors(type, tag, afterPosition)

                tr.getRange(startKeySelector, endSelector, LIMIT_ONE)
                    .asList()
                    .thenApply { keyValues ->
                        keyValues.map {
                            Tuple.fromBytes(it.key).getLastAsUuid()
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
