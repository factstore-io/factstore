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

    override suspend fun findById(request: FindByIdRequest): FindByIdResult =
        db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(request.storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindByIdResult.StoreNotFound(request.storeName))
                    } else {
                        request.factId.loadFact(storeId).thenApply { fact ->
                            if (fact != null) FindByIdResult.Found(fact) else FindByIdResult.NotFound(request.factId)
                        }
                    }
                }
            }
        }.await()

    override suspend fun existsById(request: ExistsByIdRequest): ExistsByIdResult =
        db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(request.storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(ExistsByIdResult.StoreNotFound(request.storeName))
                    } else {
                        request.factId.existsById(storeId).thenApply { exists ->
                            if (exists) ExistsByIdResult.Exists else ExistsByIdResult.DoesNotExist
                        }
                    }
                }
            }
        }.await()

    override suspend fun findInTimeRange(request: FindInTimeRangeRequest): FindInTimeRangeResult {
        val start = request.timeRange.start
        val end = request.timeRange.end

        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(request.storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindInTimeRangeResult.StoreNotFound(request.storeName))
                    } else {
                        val begin = createdAtIndexSubspace.getKey(storeId, start)
                        val endKey = createdAtIndexSubspace.getKey(storeId, end)

                        tr.getRange(begin, endKey, request.limit.toFdbLimit(), request.direction.isReverse())
                            .asList().thenCompose { kvs ->
                                val factFutures: List<CompletableFuture<FdbFact?>> = kvs.map { kv ->
                                    val factPosition = createdAtIndexSubspace.unpackPosition(kv.key)
                                    tr.run { factPosition.lookupFact(storeId) }
                                }

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

    override suspend fun findBySubject(request: FindBySubjectRequest): FindBySubjectResult {
        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(request.storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindBySubjectResult.StoreNotFound(request.storeName))
                    } else {
                        val subjectRange = subjectIndexSubspace.range(storeId, request.subject)

                        tr.getRange(subjectRange, request.limit.toFdbLimit(), request.direction.isReverse())
                            .asList().thenCompose { kvs ->
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

    override suspend fun findByTags(request: FindByTagsRequest): FindByTagsResult {
        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(request.storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindByTagsResult.StoreNotFound(request.storeName))
                    } else {
                        when {
                            request.tags.isEmpty() -> CompletableFuture.completedFuture(FindByTagsResult.Found(emptyList()))
                            request.tags.size == 1 -> {
                                val (key, value) = request.tags.first()
                                val range = tagsIndexSubspace.range(storeId, key, value)
                                tr.getRange(range, request.limit.toFdbLimit(), request.direction.isReverse())
                                    .asList().thenCompose { kvs ->
                                        val loadFutures = kvs.map { kv ->
                                            val factPosition = tagsIndexSubspace.unpackPosition(kv.key)
                                            factPosition.lookupFact(storeId)
                                        }
                                        CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                                            val facts = loadFutures.mapNotNull { it.resultNow()?.fact }
                                            FindByTagsResult.Found(facts)
                                        }
                                    }
                            }
                            else -> {
                                // Multiple tags — intersection must happen in application code first,
                                // so limit and direction cannot be pushed into FDB natively
                                val tagFutures: List<CompletableFuture<Set<FactPosition>>> = request.tags.map { (key, value) ->
                                    val range = tagsIndexSubspace.range(storeId, key, value)
                                    tr.getRange(range).asList().thenApply { kvs ->
                                        kvs.mapTo(mutableSetOf()) { kv ->
                                            tagsIndexSubspace.unpackPosition(kv.key)
                                        }
                                    }
                                }

                                CompletableFuture.allOf(*tagFutures.toTypedArray()).thenCompose {
                                    val intersectedPositions: List<FactPosition> = tagFutures
                                        .map { it.getNow(emptySet()) }
                                        .reduce { acc, positions -> acc intersect positions }
                                        .sortedWith(request.direction.toComparator())
                                        .let { sorted -> request.limit.value?.let { sorted.take(it) } ?: sorted }

                                    val loadFutures = intersectedPositions.map { it.lookupFact(storeId) }

                                    CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                                        val facts = loadFutures
                                            .mapNotNull { it.resultNow() }
                                            .sortedWith(request.direction.toFdbFactComparator())
                                            .map { it.fact }
                                        FindByTagsResult.Found(facts)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }.await()
    }

    override suspend fun findByTagQuery(request: FindByTagQueryRequest): FindByTagQueryResult {
        return db.readAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(request.storeName).thenCompose { storeId ->
                    if (storeId == null) {
                        CompletableFuture.completedFuture(FindByTagQueryResult.StoreNotFound(request.storeName))
                    } else {
                        with(tr.snapshot()) {
                            val queryItemFutures = request.query.queryItems
                                .map { queryItem ->
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
                }.toSet()
            }
        }
        return CompletableFuture.allOf(*futures.toTypedArray()).thenApply {
            futures
                .map { it.getNow(emptySet()) }
                .reduce { acc, set -> acc.union(set) }
                .orEmpty()
        }
    }

    context(tr: ReadTransaction, storeId: StoreId)
    private fun TagTypeItem.resolveFactPositions(): CompletableFuture<Set<FactPosition>> {
        val futures: List<CompletableFuture<Set<FactPosition>>> = types.map { type ->
            val tagFutures = tags.map { tag ->
                val range = tagsTypeIndexSubspace.range(storeId, type, tag)
                tr.getRange(range).asList().thenApply { keyValues ->
                    keyValues.map {
                        tagsTypeIndexSubspace.unpackPosition(it.key)
                    }.toSet()
                }
            }

            CompletableFuture.allOf(*tagFutures.toTypedArray()).thenApply {
                tagFutures
                    .map { it.getNow(emptySet()) }
                    .reduce { acc, set -> acc.intersect(set) }
                    .orEmpty()
            }
        }

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
