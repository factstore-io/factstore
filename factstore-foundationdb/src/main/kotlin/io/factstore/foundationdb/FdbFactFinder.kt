package io.factstore.foundationdb

import com.apple.foundationdb.ReadTransaction
import io.factstore.core.*
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

class FdbFactFinder(private val fdbFactStore: FdbFactStore) : FactFinder {

    private val db = fdbFactStore.db

    private val factPositionSubspace = fdbFactStore.context.factPositionIndexSubspace

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
