package io.factstore.foundationdb

import io.factstore.core.RemoveStoreRequest
import io.factstore.core.RemoveStoreResult
import io.factstore.core.StoreRemover
import kotlinx.coroutines.future.await

class FdbStoreRemover(
    private val fdbFactStore: FdbFactStore
) : StoreRemover {

    override suspend fun handle(request: RemoveStoreRequest): RemoveStoreResult {
        return fdbFactStore.db.runAsync { tr ->
            with(tr) {
                fdbFactStore.context.lookUpStoreIdByName(request.storeName).thenApply { storeId ->
                    if (storeId == null) {
                        RemoveStoreResult.StoreNotFound(request.storeName)
                    } else {
                        with(fdbFactStore.context) {
                            storeNameToIdIndex.clear(request.storeName)
                            storeSubspace.clear(storeId)
                            headSubspace.clear(storeId)
                            factSubspace.clearRange(storeId)
                            factPositionIndexSubspace.clearRange(storeId)
                            eventTypeIndexSubspace.clearRange(storeId)
                            createdAtIndexSubspace.clearRange(storeId)
                            subjectIndexSubspace.clearRange(storeId)
                            metadataIndexSubspace.clearRange(storeId)
                            tagsIndexSubspace.clearRange(storeId)
                            tagsTypeIndexSubspace.clearRange(storeId)
                            idempotencySubspace.clearRange(storeId)
                        }
                        RemoveStoreResult.StoreRemoved(request.storeName)
                    }
                }
            }
        }.await()
    }

}
