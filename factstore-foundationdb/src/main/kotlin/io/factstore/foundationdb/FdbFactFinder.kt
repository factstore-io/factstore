package io.factstore.foundationdb

import com.apple.foundationdb.ReadTransaction
import io.factstore.core.*
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

class FdbFactFinder(private val fdbFactStore: FdbFactStore) : FactFinder {

    private val db = fdbFactStore.db

    private val factPositionSubspace = fdbFactStore.context.factPositionIndexSubspace

    private val createdAtIndexSubspace = fdbFactStore.context.createdAtIndexSubspace
    private val subjectIndexSubspace = fdbFactStore.context.subjectIndexSubspace
    private val tagsIndexSubspace = fdbFactStore.context.tagsIndexSubspace
    private val tagsTypeIndexSubspace = fdbFactStore.context.tagsTypeIndexSubspace

    override suspend fun findById(storeName: StoreName, factId: FactId): FindByIdResult =
        db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindByIdResult.StoreNotFound)
                    } else {
                        factId.loadFact(storeId).thenApply { fact ->
                            if (fact != null) {
                                FindByIdResult.Found(fact)
                            } else {
                                FindByIdResult.NotFound(factId)
                            }
                        }
                    }
                }
            }
        }.await()


    override suspend fun existsById(storeName: StoreName, factId: FactId): ExistsByIdResult =
        db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(ExistsByIdResult.StoreNotFound)
                    } else {
                        factId.existsById(storeId).thenApply { exists ->
                            if (exists) {
                                ExistsByIdResult.Exists
                            } else {
                                ExistsByIdResult.DoesNotExist
                            }
                        }
                    }
                }
            }
        }.await()

    override suspend fun findInTimeRange(storeName: StoreName, timeRange: TimeRange): FindInTimeRangeResult {
        val start = timeRange.start
        val end = timeRange.end

        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindInTimeRangeResult.StoreNotFound)
                    } else {
                        val begin = createdAtIndexSubspace.getKey(storeId, start)
                        val endKey = createdAtIndexSubspace.getKey(storeId, end)

                        tr.getRange(begin, endKey).asList().thenCompose { kvs ->
                            val factFutures: List<CompletableFuture<FdbFact?>> = kvs.map { kv ->
                                val factPosition = createdAtIndexSubspace.unpackPosition(kv.key)
                                tr.run { factPosition.lookupFact(storeId) }
                            }

                            // wait for all facts to complete
                            CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                                val facts = factFutures.mapNotNull { it.resultNow()?.fact }
                                FindInTimeRangeResult.Found(facts)
                            }
                        }
                    }
                }
            }
        }.await()
    }

    override suspend fun findBySubject(storeName: StoreName, subjectRef: SubjectRef): FindBySubjectResult {
        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindBySubjectResult.StoreNotFound)
                    } else {
                        val subjectRange = subjectIndexSubspace.range(storeId, subjectRef)
                        tr.getRange(subjectRange).asList().thenCompose { kvs ->
                            val factFutures: List<CompletableFuture<FdbFact?>> = kvs.map { kv ->
                                val factPosition = subjectIndexSubspace.unpackPosition(kv.key)
                                tr.run { factPosition.lookupFact(storeId) }
                            }

                            CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                                val facts = factFutures.mapNotNull { it.resultNow()?.fact }
                                FindBySubjectResult.Found(facts)
                            }
                        }
                    }
                }
            }
        }.await()
    }

    override suspend fun findByTags(storeName: StoreName, tags: List<Pair<TagKey, TagValue>>): FindByTagsResult {
        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindByTagsResult.StoreNotFound)
                    } else {
                        if (tags.isEmpty()) return@thenCompose CompletableFuture.completedFuture(
                            FindByTagsResult.Found(
                                emptyList()
                            )
                        )

                        // For each (key, value) pair, get matching factIds
                        val tagFutures: List<CompletableFuture<Set<FactPosition>>> = tags.map { (key, value) ->
                            val range = tagsIndexSubspace.range(storeId, key, value)
                            tr.getRange(range).asList().thenApply { kvs ->
                                kvs.mapTo(mutableSetOf()) { kv ->
                                    tagsIndexSubspace.unpackPosition(kv.key)
                                }
                            }
                        }

                        // Once all tag lookups finish, union them and load facts
                        CompletableFuture.allOf(*tagFutures.toTypedArray()).thenCompose {
                            val allFactPositions: Set<FactPosition> = tagFutures
                                .flatMap { it.getNow(emptySet()) }
                                .toSet() // OR semantics = union

                            with(tr) {
                                val loadFutures: List<CompletableFuture<FdbFact?>> =
                                    allFactPositions.map { it.lookupFact(storeId) }

                                CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                                    val facts = loadFutures
                                        .mapNotNull { it.resultNow() }
                                        .sortedBy { it.factPosition }
                                        .map { it.fact }
                                    FindByTagsResult.Found(facts)
                                }
                            }
                        }
                    }
                }
            }

        }.await()
    }

    override suspend fun findByTagQuery(storeName: StoreName, query: TagQuery): FindByTagQueryResult {
        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindByTagQueryResult.StoreNotFound)
                    } else {
                        with(tr.snapshot()) {
                            val queryItemFutures = query.queryItems
                                .map { queryItem ->
                                    // map query item to list of fact IDs
                                    storeId.run { queryItem.resolveFactPositions() }
                                }

                            CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenCompose {
                                val allFactPositions: Set<FactPosition> = queryItemFutures
                                    .flatMap { it.getNow(emptySet()) }
                                    .toSet() // OR semantics = union

                                val loadFutures: List<CompletableFuture<FdbFact?>> =
                                    allFactPositions.map { it.lookupFact(storeId) }

                                CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                                    val facts = loadFutures
                                        .mapNotNull { it.getNow(null) }
                                        .sortedBy { it.factPosition }
                                        .map { it.fact }
                                    FindByTagQueryResult.Found(facts)
                                }
                            }
                        }
                    }
                }
            }
        }.await()
    }

    context(tr: ReadTransaction, storeId: StoreId)
    private fun TagQueryItem.resolveFactPositions(): CompletableFuture<Set<FactPosition>> = when (this) {
        is TagTypeItem -> resolveFactPositions()
        is TagOnlyQueryItem -> resolveFactPositions()
    }

    context(tr: ReadTransaction, storeId: StoreId)
    private fun TagOnlyQueryItem.resolveFactPositions(): CompletableFuture<Set<FactPosition>> {
        val futures: List<CompletableFuture<Set<FactPosition>>> = tags.map { tag ->
            val range = tagsIndexSubspace.range(storeId, tag)
            tr.getRange(range).asList().thenApply { keyValues ->
                keyValues.map {
                    tagsIndexSubspace.unpackPosition(it.key)
                }.toSet() // Convert to a Set to easily combine results
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

    context(tr: ReadTransaction, storeId: StoreId)
    private fun TagTypeItem.resolveFactPositions(): CompletableFuture<Set<FactPosition>> {
        // use composite "type+tag" index
        val futures: List<CompletableFuture<Set<FactPosition>>> = types.map { type ->
            val tagFutures = tags.map { tag ->
                val range = tagsTypeIndexSubspace.range(storeId, type, tag)
                tr.getRange(range).asList().thenApply { keyValues ->
                    keyValues.map {
                        tagsTypeIndexSubspace.unpackPosition(it.key)
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

    context(transaction: ReadTransaction)
    private fun FactId.loadFact(storeId: StoreId): CompletableFuture<Fact?> {
        return with(fdbFactStore) {
            storeId.run { this@loadFact.loadFactById().thenApply { it?.fact } }
        }
    }

    context(transaction: ReadTransaction)
    private fun FactId.existsById(storeId: StoreId): CompletableFuture<Boolean> {
        return factPositionSubspace.exists(storeId, this)
    }

    context(tr: ReadTransaction)
    private fun FactPosition.lookupFact(storeId: StoreId): CompletableFuture<FdbFact?> {
        return with(fdbFactStore) {
            storeId.run { this@lookupFact.loadFactByPosition() }
        }
    }

}
