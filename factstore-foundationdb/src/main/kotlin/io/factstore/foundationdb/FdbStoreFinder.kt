package io.factstore.foundationdb

import io.factstore.core.ExistsStoreByNameRequest
import io.factstore.core.ExistsStoreByNameResult
import io.factstore.core.FindStoreByNameRequest
import io.factstore.core.FindStoreByNameResult
import io.factstore.core.StoreFinder
import io.factstore.core.StoreMetadata

internal class FdbStoreFinder(storage: FdbStorage) : StoreFinder, FdbAccess by storage {

    override suspend fun listStores(): List<StoreMetadata> = inReadTransaction { keyspace.storeSubspace.all() }

    override suspend fun existsStoreByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult = inReadTransaction {
        if (storeIdOf(request.storeName) != null) ExistsStoreByNameResult.Exists
        else ExistsStoreByNameResult.DoesNotExist
    }

    override suspend fun findStoreByName(request: FindStoreByNameRequest): FindStoreByNameResult = inReadTransaction {
        val id = storeIdOf(request.storeName)
            ?: return@inReadTransaction FindStoreByNameResult.NotFound(request.storeName)

        val metadata = metadataOf(id)
            ?: error("Store name '${request.storeName.value}' resolved to ID $id " +
                "but no metadata record exists — index/metadata out of sync")
        FindStoreByNameResult.Found(metadata)
    }
}
