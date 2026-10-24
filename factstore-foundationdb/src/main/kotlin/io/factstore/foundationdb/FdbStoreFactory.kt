package io.factstore.foundationdb

import io.factstore.core.*
import java.time.Instant

class FdbStoreFactory(
    private val store: FdbFactStore
) : StoreFactory {

    override suspend fun create(request: CreateStoreRequest): CreateStoreResult = store.db.inTransaction {
        if (store.context.lookUpStoreIdByName(request.storeName) != null) {
            return@inTransaction CreateStoreResult.NameAlreadyExists(request.storeName)
        }

        val id = StoreId.generate()
        val createdAt = Instant.now()
        store.context.saveMetadata(
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
