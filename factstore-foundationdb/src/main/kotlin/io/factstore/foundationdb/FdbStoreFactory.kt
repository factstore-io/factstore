package io.factstore.foundationdb

import io.factstore.core.*
import kotlinx.coroutines.future.await
import java.time.Instant

class FdbStoreFactory(
    private val store: FdbFactStore
) : StoreFactory {

    override suspend fun handle(request: CreateStoreRequest): CreateStoreResult {
        // create...
        return store.db.runAsync { tr ->
            with(tr) {
                // check if name is already taken
                store.context.lookUpStoreIdByName(request.storeName).thenApply { id ->
                    if (id != null) {
                        return@thenApply CreateStoreResult.NameAlreadyExists(request.storeName)
                    } else {
                        val id = StoreId.generate()
                        val metadata = FdbStoreMetadata(
                            storeId = id.uuid,
                            name = request.storeName.value,
                            createdAtEpochSeconds = Instant.now().epochSecond
                        )
                        store.context.saveMetadata(metadata, tr)
                        CreateStoreResult.Created(id)
                    }
                }
            }
        }.await()
    }

}
