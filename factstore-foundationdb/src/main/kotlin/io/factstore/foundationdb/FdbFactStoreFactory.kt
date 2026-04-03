package io.factstore.foundationdb

import io.factstore.core.*
import kotlinx.coroutines.future.await
import java.time.Instant

class FdbFactStoreFactory(
    private val store: FdbFactStore
) : FactStoreFactory {

    override suspend fun handle(request: CreateFactStoreRequest): CreateFactStoreResult {
        // create...
        return store.db.runAsync { tr ->
            // check if name is already taken
            store.context.lookUpFactstoreIdByName(request.factStoreName, tr).thenApply { id ->
                if (id != null) {
                    return@thenApply CreateFactStoreResult.NameAlreadyExists(request.factStoreName)
                } else {
                    val id = FactStoreId.generate()
                    val metadata = FdbFactStoreMetadata(
                        storeId = id.uuid,
                        name = request.factStoreName.value,
                        createdAtEpochSeconds = Instant.now().epochSecond
                    )
                    store.context.saveMetadata(metadata, tr)
                    CreateFactStoreResult.Created(id)
                }
            }
        }.await()
    }

}
