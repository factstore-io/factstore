package io.factstore.foundationdb

import io.factstore.core.RemoveStoreRequest
import io.factstore.core.RemoveStoreResult
import io.factstore.core.StoreRemover

class FdbStoreRemover(
    private val fdbFactStore: FdbFactStore
) : StoreRemover {

    override suspend fun remove(request: RemoveStoreRequest): RemoveStoreResult = fdbFactStore.db.inTransaction {
        val storeId = fdbFactStore.context.lookUpStoreIdByName(request.storeName)
            ?: return@inTransaction RemoveStoreResult.StoreNotFound(request.storeName)

        with(fdbFactStore.context) {
            storeNameToIdIndex.clear(request.storeName)
            storeSubspace.clear(storeId)
            headSubspace.clear(storeId)
            factSubspace.clearRange(storeId)
            factPositionIndexSubspace.clearRange(storeId)
            eventTypeIndexSubspace.clearRange(storeId)
            subjectIndexSubspace.clearRange(storeId)
            tagsIndexSubspace.clearRange(storeId)
            tagsTypeIndexSubspace.clearRange(storeId)
            idempotencySubspace.clearRange(storeId)
        }
        RemoveStoreResult.StoreRemoved(request.storeName)
    }

}
