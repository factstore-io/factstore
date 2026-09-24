package io.factstore.foundationdb

import com.apple.foundationdb.ReadTransaction
import io.factstore.core.*
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

class FdbFactFinder(private val fdbFactStore: FdbFactStore) : FactFinder {

    private val db = fdbFactStore.db

    private val factPositionSubspace = fdbFactStore.context.factPositionIndexSubspace

    private val createdAtIndexSubspace = fdbFactStore.context.createdAtIndexSubspace

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
                        val storeRange = createdAtIndexSubspace.range(storeId)
                        val begin = start?.let { createdAtIndexSubspace.getKey(storeId, it) } ?: storeRange.begin
                        val endKey = end?.let { createdAtIndexSubspace.getKey(storeId, it) } ?: storeRange.end

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
