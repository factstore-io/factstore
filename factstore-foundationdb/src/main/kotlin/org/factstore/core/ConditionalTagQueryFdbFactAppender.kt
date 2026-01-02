package org.factstore.core

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import kotlinx.coroutines.future.await
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.collections.orEmpty

const val LIMIT_ONE = 1
const val OR_EQUAL = true
const val ZERO_OFFSET = 0
const val ERROR_MESSAGE = "TagQueryBasedAppendCondition not met"

class ConditionalTagQueryFdbFactAppender(
    private val store: FdbFactStore
) : ConditionalTagQueryFactAppender {

    private val tagsIndexSubspace = store.tagsIndexSubspace
    private val tagsTypeIndexSubspace = store.tagsTypeIndexSubspace

    override suspend fun append(
        facts: List<Fact>,
        condition: TagQueryBasedAppendCondition
    ) {
        store.db.runAsync { tr ->
            condition.evaluate(tr).thenApply { isPreconditionMet ->
                if (isPreconditionMet) {
                    facts.store(tr)
                } else {
                    throw AppendConditionViolationException(ERROR_MESSAGE)
                }
            }
        }.await()
    }

    private fun List<Fact>.store(transaction: Transaction) = with(store) {
        this@store.store(transaction)
    }

    private fun TagQueryBasedAppendCondition.evaluate(tr: Transaction): CompletableFuture<Boolean> {
        return if (after != null) {
            after!!.getPosition(tr).thenCompose { position ->
                queryItemsForPosition(tr, position)
            }
        } else {
            queryItemsForPosition(tr)
        }
    }

    private fun FactId.getPosition(transaction: ReadTransaction) = with(store) {
        this@getPosition.getPosition(transaction)
    }

    private fun TagQueryBasedAppendCondition.queryItemsForPosition(
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
    ): CompletableFuture<Set<UUID>> = when(this) {
        is TagOnlyQueryItem -> queryByTags(tr, afterPosition)
        is TagTypeItem -> queryByTypeAndTags(tr, afterPosition)
    }

    private fun TagOnlyQueryItem.queryByTags(
        tr: ReadTransaction,
        afterPosition: Pair<Versionstamp, Long>?
    ): CompletableFuture<Set<UUID>> {

        // Helper function to create begin and end selectors for the range query
        fun createSelectors(
            tag: Pair<String, String>,
            afterPosition: Pair<Versionstamp, Long>?
        ): Pair<KeySelector, KeySelector> {
            val tuple = if (afterPosition != null) {
                // If there's a afterPosition, include it in the tuple
                Tuple.from(tag.first, tag.second, afterPosition.first, afterPosition.second)
            } else {
                // If there's no afterPosition, just use the tag
                Tuple.from(tag.first, tag.second)
            }

            // Create the beginSelector (first greater than if afterPosition is provided)
            val beginSelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(tagsIndexSubspace.pack(tuple))
            } else {
                KeySelector(tagsIndexSubspace.pack(tuple), OR_EQUAL, ZERO_OFFSET)
            }

            // Create the end selector based on the tag range
            val range = tagsIndexSubspace.range(Tuple.from(tag.first, tag.second))
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
        fun createSelectors(type: String, tag: Pair<String, String>, afterPosition: Pair<Versionstamp, Long>?): Pair<KeySelector, KeySelector> {
            val tuple = if (afterPosition != null) {
                Tuple.from(type, tag.first, tag.second, afterPosition.first, afterPosition.second)
            } else {
                Tuple.from(type, tag.first, tag.second)
            }

            val startKeySelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(tagsTypeIndexSubspace.pack(tuple))
            } else {
                KeySelector(tagsTypeIndexSubspace.pack(tuple), OR_EQUAL, ZERO_OFFSET)
            }

            val range = tagsTypeIndexSubspace.subspace(Tuple.from(type, tag.first, tag.second)).range()
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

