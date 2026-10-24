package io.factstore.foundationdb

import io.factstore.core.*
import java.time.Instant

internal class FdbStoreFactory(storage: FdbStorage) : StoreFactory, FdbAccess by storage {

    override suspend fun create(request: CreateStoreRequest): CreateStoreResult = inTransaction {
        if (storeIdOf(request.storeName) != null) {
            return@inTransaction CreateStoreResult.NameAlreadyExists(request.storeName)
        }

        val id = StoreId.generate()
        val createdAt = Instant.now()
        saveMetadata(
            FdbStoreMetadata(
                storeId = id.uuid,
                name = request.storeName.value,
                createdAtEpochSeconds = createdAt.epochSecond,
                createdAtNanos = createdAt.nano,
            )
        )
        CreateStoreResult.Created(id)
    }

}
