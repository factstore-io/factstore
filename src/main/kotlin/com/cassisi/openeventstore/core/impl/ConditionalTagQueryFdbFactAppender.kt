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

class ConditionalTagQueryFdbFactAppender(
    private val store: FdbFactStore
) : ConditionalTagQueryFactAppender {

    private val positionSubspace = store.positionSubspace
    private val tagsIndexSubspace = store.tagsIndexSubspace
    private val tagsTypeIndexSubspace = store.tagsTypeIndexSubspace
    private val typesIndexSubspace = store.eventTypeIndexSubspace

    override suspend fun append(
        facts: List<Fact>,
        condition: TagQueryBasedAppendCondition
    ) {
        store.db.runAsync { tr ->
            // evaluate condition

            condition.evaluate(tr).thenApply { safeAppend ->
                if (safeAppend) {
                    with (store) {
                        facts.forEachIndexed { index, fact ->
                            tr.store(fact, index)
                        }
                    }

                } else {
                    throw RuntimeException("Conditional append failed...")
                }
            }
        }.await()
    }

    private fun TagQueryBasedAppendCondition.evaluate(tr: Transaction): CompletableFuture<Boolean> {
        val positionFuture = after?.let { factId ->
            tr[positionSubspace.pack(Tuple.from(factId))].thenApply {
                it?.let { bytes ->
                    val positionTuple = Tuple.fromBytes(bytes)
                    Pair(positionTuple.getVersionstamp(0), positionTuple.getLong(1))
                } ?: throw RuntimeException("Fact does not exist!")
            }
        }

        return positionFuture?.thenCompose { position ->
            queryItemsForPosition(tr, position)
        } ?: queryItemsForPosition(tr)
    }

    private fun TagQueryBasedAppendCondition.queryItemsForPosition(
        tr: Transaction,
        fromPosition: Pair<Versionstamp, Long>? = null
    ): CompletableFuture<Boolean> {
        val queryItemFutures = failIfEventsMatch.queryItems.map { queryItem ->
            queryItem.resolveFactIds(tr, fromPosition)
        }

        return CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenApply {
            val factIds = queryItemFutures
                .flatMap { it.getNow(emptySet()) }
                .toSet()  // OR semantics = union

            factIds.isEmpty()
        }
    }

//    private fun TagQueryBasedAppendCondition.evaluate(tr: Transaction): CompletableFuture<Boolean> {
//        val result: CompletableFuture<Boolean> = after?.let { factId ->
//            tr[positionSubspace.pack(Tuple.from(factId))].thenApply {
//                println("read position = $it")
//                if (it != null) {
//                    val positionTuple = Tuple.fromBytes(it)
//                    println("Fact does exist with position $positionTuple")
//                    Pair(positionTuple.getVersionstamp(0), positionTuple.getLong(1))
//                } else {
//                    println("throwing...")
//                    throw RuntimeException("Fact does not exist!")
//                }
//            }
//        }?.thenCompose { position ->
//
//            println("continue with position $position")
//
//            // reexcute query with tag query from position
//            val queryItemFutures = this@evaluate.failIfEventsMatch.queryItems
//                .map { queryItem ->
//                    queryItem.resolveFactIds(tr, position)
//                }
//
//            CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenApply {
//
//
//
//                val factIds = queryItemFutures
//                    .flatMap { it.getNow(emptySet()) }
//                    .toSet() // OR semantics = union
//
//                println("all facts resolved $factIds")
//
//                factIds.isEmpty()
//            }
//
//        } ?: run {
//            // resolve query directly, fail if at least one fact appears
//
//            println("in run... $after")
//
//            // reexcute query with tag query from position
//            val queryItemFutures = this@evaluate.failIfEventsMatch.queryItems
//                .map { queryItem ->
//                    queryItem.resolveFactIds(tr)
//                }
//
//            CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenApply {
//                val factIds = queryItemFutures
//                    .flatMap { it.getNow(emptySet()) }
//                    .toSet() // OR semantics = union
//
//                factIds.isEmpty()
//            }
//        }
//        return result
//    }


    private fun FactQueryItem.resolveFactIds(
        tr: ReadTransaction,
        fromPosition: Pair<Versionstamp, Long>? = null
    ): CompletableFuture<Set<UUID>> {
        val hasTags = this.tags.isNotEmpty()
        val hasTypes = this.types.isNotEmpty()

        return when {
            hasTags && hasTypes -> {
                // use composite "type+tag" index
                val futures: List<CompletableFuture<Set<UUID>>> = types.map { type ->
                    val tagFutures = tags.map { tag ->

                        val startKeySelector = if (fromPosition != null) {
                            val tuple = Tuple.from(type, tag.first, tag.second, fromPosition.first, fromPosition.second)
                            KeySelector.firstGreaterThan(tagsTypeIndexSubspace.pack(tuple))
                        } else {
                            val tuple = Tuple.from(type, tag.first, tag.second)
                            KeySelector(tagsTypeIndexSubspace.pack(tuple), true, 0)
                        }
                        val range = tagsTypeIndexSubspace.subspace(Tuple.from(type, tag.first, tag.second)).range()
                        val endSelector = KeySelector.lastLessOrEqual(range.end)
                        tr.getRange(
                            startKeySelector,
                            endSelector,
                            1
                        ).asList().thenApply { keyValues ->
                            keyValues.map {
                                val tuple = tagsTypeIndexSubspace.unpack(it.key)
                                tuple.getUUID(tuple.size() - 1)
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
                CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                    futures
                        .map { it.getNow(emptySet()) }
                        .reduce { acc, set -> acc.union(set) }
                        .orEmpty()
                }
            }

            hasTags -> {
                val futures: List<CompletableFuture<Set<UUID>>> = tags.map { tag ->
                    val range = tagsIndexSubspace.range(Tuple.from(tag.first, tag.second))
                    tr.getRange(range).asList().thenApply { keyValues ->
                        keyValues.map {
                            val tuple = tagsIndexSubspace.unpack(it.key)
                            tuple.getUUID(tuple.size() - 1)
                        }.toSet() // Convert to a Set to easily combine results
                    }
                }
                // After all futures complete, perform the union of the sets
                CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                    // Union the sets from all futures
                    futures
                        .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                        .reduce { acc, set -> acc.union(set) } // Union all sets to get all matching fact IDs
                        .orEmpty() // Return empty set if no sets are present
                }
            }

            hasTypes -> {
                val futures: List<CompletableFuture<Set<UUID>>> = types.map { type ->
                    val range = typesIndexSubspace.range(Tuple.from(type))
                    tr.getRange(range).asList().thenApply { keyValues ->
                        keyValues.map {
                            val tuple = typesIndexSubspace.unpack(it.key)
                            tuple.getUUID(tuple.size() - 1)
                        }.toSet() // Convert to a Set to easily combine results
                    }
                }
                // After all futures complete, perform the union of the sets
                CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
                    futures
                        .map { it.getNow(emptySet()) } // Extract the result of each CompletableFuture
                        .reduce { acc, set -> acc.union(set) } // Union all sets to get all matching fact IDs
                        .orEmpty() // Return empty set if no sets are present
                }
            }

            else -> CompletableFuture.completedFuture(emptySet())
        }

    }

}

