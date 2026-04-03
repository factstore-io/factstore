package io.factstore.foundationdb

import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.FactStoreFinder
import io.factstore.core.FactStoreId
import io.factstore.core.FactStoreMetadata
import io.factstore.core.FactStoreName
import kotlinx.coroutines.future.await
import kotlinx.serialization.decodeFromByteArray
import java.time.Instant
import java.util.concurrent.CompletableFuture

class FdbFactStoreFinder(
    val store: FdbFactStore
) : FactStoreFinder {


    override suspend fun listAll(): List<FactStoreMetadata> {
        return store.db.readAsync { tr ->
            val storeRange = store.context.storeSubspace.range()
            tr.getRange(storeRange).asList()
                .thenApply { kvList ->
                    kvList.map { kv ->
                        val fdbMetadata = Avro.decodeFromByteArray<FdbFactStoreMetadata>(kv.value)

                        FactStoreMetadata(
                            id = FactStoreId(fdbMetadata.storeId),
                            name = FactStoreName(fdbMetadata.name),
                            createdAt = Instant.ofEpochSecond(fdbMetadata.createdAtEpochSeconds)
                        )
                    }
                }
        }.await()
    }

    override suspend fun existsByName(name: FactStoreName): Boolean {
        return store.db.readAsync { tr ->
            store.context.lookUpFactstoreIdByName(name, tr).thenApply { it != null }
        }.await()
    }

    override suspend fun findByName(name: FactStoreName): FactStoreMetadata? {
        return store.db.readAsync { tr ->

            store.context.lookUpFactstoreIdByName(name, tr).thenCompose { id ->
                id?.let {
                    store.context.getMetadata(id, tr)
                } ?: CompletableFuture.completedFuture(null)
            }.thenApply { fdbMetadata ->
                fdbMetadata?.let {
                    FactStoreMetadata(
                        id = FactStoreId(it.storeId),
                        name = FactStoreName(it.name),
                        createdAt = Instant.ofEpochSecond(it.createdAtEpochSeconds)
                    )
                }
            }
        }.await()
    }
}