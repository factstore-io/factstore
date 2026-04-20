package io.factstore.foundationdb

import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.tuple.Tuple
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

    override suspend fun findById(factStoreId: FactStoreId, factId: FactId): FindByIdResult =
        db.readAsync { tr ->
            with(tr) {
                factId.loadFact(factStoreId).thenCompose { fact ->
                    if (fact != null) {
                        CompletableFuture.completedFuture(FindByIdResult.Found(fact))
                    } else {
                        fdbFactStore.context.getMetadata(factStoreId, tr).thenApply { metadata ->
                            if (metadata == null) FindByIdResult.FactstoreNotFound else FindByIdResult.NotFound(factId)
                        }
                    }
                }
            }
        }.await()


    override suspend fun existsById(factStoreId: FactStoreId, factId: FactId): ExistsByIdResult =
        db.readAsync { tr ->
            with(tr) {
                factId.existsById(factStoreId).thenCompose { exists ->
                    if (exists) {
                        CompletableFuture.completedFuture(ExistsByIdResult.Exists)
                    } else {
                        fdbFactStore.context.getMetadata(factStoreId, tr).thenApply { metadata ->
                            if (metadata == null) ExistsByIdResult.FactstoreNotFound else ExistsByIdResult.DoesNotExist
                        }
                    }
                }
            }
        }.await()

    override suspend fun findInTimeRange(factStoreId: FactStoreId, timeRange: TimeRange): FindInTimeRangeResult {
        val start = timeRange.start
        val end = timeRange.end

        return db.readAsync { tr ->
            fdbFactStore.context.getMetadata(factStoreId, tr).thenCompose { metadata ->
                if (metadata == null) {
                    CompletableFuture.completedFuture(FindInTimeRangeResult.FactstoreNotFound)
                } else {
                    val begin = createdAtIndexSubspace.getKey(factStoreId, start)
                    val endKey = createdAtIndexSubspace.getKey(factStoreId, end)

                    tr.getRange(begin, endKey).asList().thenCompose { kvs ->
                        val factFutures: List<CompletableFuture<FdbFact?>> = kvs.map { kv ->
                            val factPosition = createdAtIndexSubspace.unpackPosition(kv.key)
                            tr.run { factPosition.lookupFact(factStoreId) }
                        }

                        // wait for all facts to complete
                        CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                            val facts = factFutures.mapNotNull { it.resultNow()?.fact }
                            FindInTimeRangeResult.Found(facts)
                        }
                    }
                }
            }
        }.await()
    }

    override suspend fun findBySubject(factStoreId: FactStoreId, subjectRef: SubjectRef): FindBySubjectResult {
        return db.readAsync { tr ->
            fdbFactStore.context.getMetadata(factStoreId, tr).thenCompose { metadata ->
                if (metadata == null) {
                    CompletableFuture.completedFuture(FindBySubjectResult.FactstoreNotFound)
                } else {
                    val subjectRange = subjectIndexSubspace.range(factStoreId, subjectRef)
                    tr.getRange(subjectRange).asList().thenCompose { kvs ->
                        val factFutures: List<CompletableFuture<FdbFact?>> = kvs.map { kv ->
                            val factPosition = subjectIndexSubspace.unpackPosition(kv.key)
                            tr.run { factPosition.lookupFact(factStoreId) }
                        }

                        CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                            val facts = factFutures.mapNotNull { it.resultNow()?.fact }
                            FindBySubjectResult.Found(facts)
                        }
                    }
                }
            }
        }.await()
    }

    override suspend fun findByTags(factStoreId: FactStoreId, tags: List<Pair<TagKey, TagValue>>): FindByTagsResult {
        return db.readAsync { tr ->
            fdbFactStore.context.getMetadata(factStoreId, tr).thenCompose { metadata ->
                if (metadata == null) {
                    CompletableFuture.completedFuture(FindByTagsResult.FactstoreNotFound)
                } else {
                    if (tags.isEmpty()) return@thenCompose CompletableFuture.completedFuture(FindByTagsResult.Found(emptyList()))

                    // For each (key, value) pair, get matching factIds
                    val tagFutures: List<CompletableFuture<Set<FactPosition>>> = tags.map { (key, value) ->
                        val range = tagsIndexSubspace.range(factStoreId, key, value)
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
                                allFactPositions.map { it.lookupFact(factStoreId) }

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
        }.await()
    }

    override suspend fun findByTagQuery(factStoreId: FactStoreId, query: TagQuery): FindByTagQueryResult {
        return db.readAsync { tr ->
            fdbFactStore.context.getMetadata(factStoreId, tr).thenCompose { metadata ->
                if (metadata == null) {
                    CompletableFuture.completedFuture(FindByTagQueryResult.FactstoreNotFound)
                } else {
                    with(tr.snapshot()) {
                        val queryItemFutures = query.queryItems
                            .map { queryItem ->
                                // map query item to list of fact IDs
                                factStoreId.run { queryItem.resolveFactPositions() }
                            }

                        CompletableFuture.allOf(*queryItemFutures.toTypedArray()).thenCompose {
                            val allFactPositions: Set<FactPosition> = queryItemFutures
                                .flatMap { it.getNow(emptySet()) }
                                .toSet() // OR semantics = union

                            val loadFutures: List<CompletableFuture<FdbFact?>> =
                                allFactPositions.map { it.lookupFact(factStoreId) }

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
        }.await()
    }

    context(tr: ReadTransaction, factStoreId: FactStoreId)
    private fun TagQueryItem.resolveFactPositions(): CompletableFuture<Set<FactPosition>> = when (this) {
        is TagTypeItem -> resolveFactPositions()
        is TagOnlyQueryItem -> resolveFactPositions()
    }

    context(tr: ReadTransaction, factStoreId: FactStoreId)
    private fun TagOnlyQueryItem.resolveFactPositions(): CompletableFuture<Set<FactPosition>> {
        val futures: List<CompletableFuture<Set<FactPosition>>> = tags.map { tag ->
            val range = tagsIndexSubspace.range(factStoreId, tag)
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

    context(tr: ReadTransaction, factStoreId: FactStoreId)
    private fun TagTypeItem.resolveFactPositions(): CompletableFuture<Set<FactPosition>> {
        // use composite "type+tag" index
        val futures: List<CompletableFuture<Set<FactPosition>>> = types.map { type ->
            val tagFutures = tags.map { tag ->
                val range = tagsTypeIndexSubspace.range(factStoreId, type, tag)
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
    private fun FactId.loadFact(factStoreId: FactStoreId): CompletableFuture<Fact?> {
        return with(fdbFactStore) {
            factStoreId.run { this@loadFact.loadFactById().thenApply { it?.fact } }
        }
    }

    context(transaction: ReadTransaction)
    private fun FactId.existsById(factStoreId: FactStoreId): CompletableFuture<Boolean> {
        return factPositionSubspace.exists(factStoreId, this)
    }

    context(tr: ReadTransaction)
    private fun FactPosition.lookupFact(factStoreId: FactStoreId): CompletableFuture<FdbFact?> {
        return with(fdbFactStore) {
            factStoreId.run { this@lookupFact.loadFactByPosition() }
        }
    }

}
