package io.factstore.foundationdb

import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.StoreFinder
import io.factstore.core.StoreId
import io.factstore.core.StoreMetadata
import io.factstore.core.StoreName
import kotlinx.coroutines.future.await
import kotlinx.serialization.decodeFromByteArray
import java.time.Instant
import java.util.concurrent.CompletableFuture

class FdbStoreFinder(
    val store: FdbFactStore
) : StoreFinder {


    override suspend fun listAll(): List<StoreMetadata> {
        return store.db.readAsync { tr ->
            val storeRange = store.context.storeSubspace.range()
            tr.getRange(storeRange).asList()
                .thenApply { kvList ->
                    kvList.map { kv ->
                        val fdbMetadata = Avro.decodeFromByteArray<FdbStoreMetadata>(kv.value)

                        StoreMetadata(
                            id = StoreId(fdbMetadata.storeId),
                            name = StoreName(fdbMetadata.name),
                            createdAt = Instant.ofEpochSecond(fdbMetadata.createdAtEpochSeconds)
                        )
                    }
                }
        }.await()
    }

    override suspend fun existsByName(name: StoreName): Boolean {
        return store.db.readAsync { tr ->
            store.context.lookUpFactstoreIdByName(name, tr).thenApply { it != null }
        }.await()
    }

    override suspend fun findByName(name: StoreName): StoreMetadata? {
        return store.db.readAsync { tr ->

            store.context.lookUpFactstoreIdByName(name, tr).thenCompose { id ->
                id?.let {
                    store.context.getMetadata(id, tr)
                } ?: CompletableFuture.completedFuture(null)
            }.thenApply { fdbMetadata ->
                fdbMetadata?.let {
                    StoreMetadata(
                        id = StoreId(it.storeId),
                        name = StoreName(it.name),
                        createdAt = Instant.ofEpochSecond(it.createdAtEpochSeconds)
                    )
                }
            }
        }.await()
    }
}