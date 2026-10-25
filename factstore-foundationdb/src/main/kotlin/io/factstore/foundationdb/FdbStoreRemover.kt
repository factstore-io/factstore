package io.factstore.foundationdb

import io.factstore.core.RemoveStoreRequest
import io.factstore.core.RemoveStoreResult
import io.factstore.core.StoreRemover

internal class FdbStoreRemover(storage: FdbStorage) : StoreRemover, FdbAccess by storage {

    override suspend fun removeStore(request: RemoveStoreRequest): RemoveStoreResult = inTransaction {
        val storeId = storeIdOf(request.storeName)
            ?: return@inTransaction RemoveStoreResult.NotFound(request.storeName)

        with(keyspace) {
            storeNameToIdIndex.clear(request.storeName)
            storeSubspace.clear(storeId)
            headSubspace.clear(storeId)
            factSubspace.clear(storeId)
            factPositionIndexSubspace.clear(storeId)
            typeIndexSubspace.clear(storeId)
            subjectIndexSubspace.clear(storeId)
            tagsIndexSubspace.clear(storeId)
            tagsTypeIndexSubspace.clear(storeId)
            idempotencySubspace.clear(storeId)
        }
        RemoveStoreResult.Removed(request.storeName)
    }

}
