package com.cassisi.openeventstore.core.impl

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.cassisi.openeventstore.core.ConditionalTagQueryFactAppender
import com.cassisi.openeventstore.core.Fact
import com.cassisi.openeventstore.core.FactQueryItem
import com.cassisi.openeventstore.core.TagQueryBasedAppendCondition
import kotlinx.coroutines.future.await
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.collections.orEmpty

const val LIMIT_ONE = 1

class ConditionalTagQueryFdbFactAppender(
    private val store: FdbFactStore
) : ConditionalTagQueryFactAppender {

    private val tagsIndexSubspace = store.tagsIndexSubspace
    private val tagsTypeIndexSubspace = store.tagsTypeIndexSubspace
    private val typesIndexSubspace = store.eventTypeIndexSubspace

    override suspend fun append(
        facts: List<Fact>,
        condition: TagQueryBasedAppendCondition
    ) {
        store.db.runAsync { tr ->
            condition.evaluate(tr).thenApply { isPreconditionMet ->
                if (isPreconditionMet) {
                    facts.store(tr)
                } else {
                    throw RuntimeException("Conditional append failed...")
                }
            }
        }.await()
    }

    private fun List<Fact>.store(transaction: Transaction) = with(store) {
        this@store.store(transaction)
    }

    private fun TagQueryBasedAppendCondition.evaluate(tr: Transaction): CompletableFuture<Boolean> {
        return if (after != null) {
            after.getPosition(tr).thenCompose { position ->
                queryItemsForPosition(tr, position)
            }
        } else {
            queryItemsForPosition(tr)
        }
    }

    private fun UUID.getPosition(transaction: ReadTransaction) = with(store) {
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


    private fun FactQueryItem.resolveFactIds(
        tr: ReadTransaction,
        afterPosition: Pair<Versionstamp, Long>? = null
    ): CompletableFuture<Set<UUID>> {
        val hasTags = this.tags.isNotEmpty()
        val hasTypes = this.types.isNotEmpty()

        return when {
            hasTags && hasTypes -> queryByTypeAndTags(afterPosition, tr)
            hasTags -> queryByTags(tr, afterPosition)
            hasTypes -> queryFromTypes(tr, afterPosition)
            else -> CompletableFuture.completedFuture(emptySet())
        }

    }

    private fun FactQueryItem.queryFromTypes(
        tr: ReadTransaction,
        afterPosition: Pair<Versionstamp, Long>?
    ): CompletableFuture<Set<UUID>?> {

        // Helper function to create begin and end selectors for the range query
        fun createSelectors(type: String, afterPosition: Pair<Versionstamp, Long>?): Pair<KeySelector, KeySelector> {
            val tuple = if (afterPosition != null) {
                // If there's a afterPosition, include it in the tuple
                Tuple.from(type, afterPosition.first, afterPosition.second)
            } else {
                // If there's no afterPosition, just use the type
                Tuple.from(type)
            }

            // Create the beginSelector (first greater than if afterPosition is provided)
            val beginSelector = if (afterPosition != null) {
                KeySelector.firstGreaterThan(typesIndexSubspace.pack(tuple))
            } else {
                KeySelector(typesIndexSubspace.pack(tuple), true, 0)
            }

            // Create the end selector based on the type range
            val range = typesIndexSubspace.range(Tuple.from(type))
            val endSelector = KeySelector.lastLessOrEqual(range.end)

            return Pair(beginSelector, endSelector)
        }

        // Use the modified logic with the afterPosition support
        val futures: List<CompletableFuture<Set<UUID>>> = types.map { type ->
            val (beginSelector, endSelector) = createSelectors(type, afterPosition)

            tr.getRange(beginSelector, endSelector, LIMIT_ONE)
                .asList()
                .thenApply { keyValues ->
                    keyValues.map {
                        typesIndexSubspace.unpack(it.key).getLastAsUuid()
                    }.toSet() // Convert to Set to easily combine results
                }
        }

        // After all futures complete, perform the union of the sets
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
            futures
                .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                .reduce { acc, set -> acc.union(set) } // Union all sets to get all matching fact IDs
                .orEmpty() // Return empty set if no sets are present
        }
    }

    private fun FactQueryItem.queryByTags(
        tr: ReadTransaction,
        afterPosition: Pair<Versionstamp, Long>?
    ): CompletableFuture<Set<UUID>?> {

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
                KeySelector(tagsIndexSubspace.pack(tuple), true, 0)
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
                        tagsIndexSubspace.unpack(it.key).getLastAsUuid()
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

    private fun FactQueryItem.queryByTypeAndTags(
        afterPosition: Pair<Versionstamp, Long>?,
        tr: ReadTransaction
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
                KeySelector(tagsTypeIndexSubspace.pack(tuple), true, 0)
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

                tr.getRange(startKeySelector, endSelector, 1)
                    .asList()
                    .thenApply { keyValues ->
                        keyValues.map {
                            tagsTypeIndexSubspace.unpack(it.key).getLastAsUuid()
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

