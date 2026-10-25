package io.factstore.foundationdb

import io.factstore.core.RemoveStoreRequest
import io.factstore.core.RemoveStoreResult
import io.factstore.core.StoreRemover

internal class FdbStoreRemover(storage: FdbStorage) : StoreRemover, FdbAccess by storage {

    override suspend fun remove(request: RemoveStoreRequest): RemoveStoreResult = inTransaction {
        val storeId = storeIdOf(request.storeName)
            ?: return@inTransaction RemoveStoreResult.StoreNotFound(request.storeName)

        with(keyspace) {
            storeNameToIdIndex.clear(request.storeName)
            storeSubspace.clear(storeId)
            headSubspace.clear(storeId)
            factSubspace.clearRange(storeId)
            factPositionIndexSubspace.clearRange(storeId)
            typeIndexSubspace.clearRange(storeId)
            subjectIndexSubspace.clearRange(storeId)
            tagsIndexSubspace.clearRange(storeId)
            tagsTypeIndexSubspace.clearRange(storeId)
            idempotencySubspace.clearRange(storeId)
        }
        RemoveStoreResult.StoreRemoved(request.storeName)
    }

}
