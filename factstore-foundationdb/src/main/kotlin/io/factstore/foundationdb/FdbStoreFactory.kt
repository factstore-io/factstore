package io.factstore.foundationdb

import io.factstore.core.*
import java.time.Instant

internal class FdbStoreFactory(storage: FdbStorage) : StoreFactory, FdbAccess by storage {

    override suspend fun createStore(request: CreateStoreRequest): CreateStoreResult = inTransaction {
        if (storeIdOf(request.storeName) != null) {
            return@inTransaction CreateStoreResult.NameAlreadyExists(request.storeName)
        }

        val id = StoreId.generate()
        saveMetadata(StoreMetadata(id = id, name = request.storeName, createdAt = Instant.now()))
        CreateStoreResult.Created(id)
    }

}
