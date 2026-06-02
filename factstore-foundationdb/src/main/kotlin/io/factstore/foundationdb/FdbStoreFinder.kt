package io.factstore.foundationdb

import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.FindStoreByNameRequest
import io.factstore.core.FindStoreByNameResult
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
            with(tr) {
                store.context.lookUpStoreIdByName(name).thenApply { it != null }
            }
        }.await()
    }

    override suspend fun findByName(request: FindStoreByNameRequest): FindStoreByNameResult {
        return store.db.readAsync { tr ->
            with(tr) {
                store.context.lookUpStoreIdByName(request.name).thenCompose { id ->
                    id?.let {
                        store.context.getMetadata(id).thenApply { metadata ->
                            if (metadata != null) {
                                FindStoreByNameResult.Found(
                                    StoreMetadata(
                                        id = StoreId(metadata.storeId),
                                        name = StoreName(metadata.name),
                                        createdAt = Instant.ofEpochSecond(metadata.createdAtEpochSeconds)
                                    )
                                )
                            } else {
                                // should not be the case.... (consider logging that)
                                FindStoreByNameResult.NotFound(request.name)
                            }
                        }
                    } ?: CompletableFuture.completedFuture(FindStoreByNameResult.NotFound(request.name))
                }
            }
        }.await()
    }
}