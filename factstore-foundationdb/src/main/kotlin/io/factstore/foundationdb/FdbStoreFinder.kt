package io.factstore.foundationdb

import io.factstore.core.ExistsStoreByNameRequest
import io.factstore.core.ExistsStoreByNameResult
import io.factstore.core.FindStoreByNameRequest
import io.factstore.core.FindStoreByNameResult
import io.factstore.core.StoreFinder
import io.factstore.core.StoreMetadata
import kotlinx.coroutines.future.await

internal class FdbStoreFinder(storage: FdbStorage) : StoreFinder, FdbAccess by storage {

    override suspend fun listAll(): List<StoreMetadata> = inReadTransaction {
        getRange(keyspace.storeSubspace.range).asList().await()
            .map { kv -> kv.value.toFdbStoreMetadata().toStoreMetadata() }
    }

    override suspend fun existsByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult = inReadTransaction {
        if (storeIdOf(request.storeName) != null) ExistsStoreByNameResult.StoreExists
        else ExistsStoreByNameResult.StoreAbsent
    }

    override suspend fun findByName(request: FindStoreByNameRequest): FindStoreByNameResult = inReadTransaction {
        val id = storeIdOf(request.storeName)
            ?: return@inReadTransaction FindStoreByNameResult.NotFound(request.storeName)

        val metadata = metadataOf(id)
            ?: error("Store name '${request.storeName.value}' resolved to ID $id " +
                "but no metadata record exists — index/metadata out of sync")
        FindStoreByNameResult.Found(metadata.toStoreMetadata())
    }
}
