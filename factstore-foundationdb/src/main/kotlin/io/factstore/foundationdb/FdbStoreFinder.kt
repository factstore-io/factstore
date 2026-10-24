package io.factstore.foundationdb

import io.factstore.core.ExistsStoreByNameRequest
import io.factstore.core.ExistsStoreByNameResult
import io.factstore.core.FindStoreByNameRequest
import io.factstore.core.FindStoreByNameResult
import io.factstore.core.StoreFinder
import io.factstore.core.StoreMetadata
import kotlinx.coroutines.future.await

class FdbStoreFinder(
    private val store: FdbFactStore
) : StoreFinder {

    override suspend fun listAll(): List<StoreMetadata> = store.db.inReadTransaction {
        getRange(store.context.storeSubspace.range()).asList().await()
            .map { kv -> kv.value.toFdbStoreMetadata().toStoreMetadata() }
    }

    override suspend fun existsByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult = store.db.inReadTransaction {
        if (store.context.lookUpStoreIdByName(request.storeName) != null) ExistsStoreByNameResult.StoreExists
        else ExistsStoreByNameResult.StoreAbsent
    }

    override suspend fun findByName(request: FindStoreByNameRequest): FindStoreByNameResult = store.db.inReadTransaction {
        val id = store.context.lookUpStoreIdByName(request.storeName)
            ?: return@inReadTransaction FindStoreByNameResult.NotFound(request.storeName)

        val metadata = store.context.getMetadata(id)
            ?: error("Store name '${request.storeName.value}' resolved to ID $id " +
                "but no metadata record exists — index/metadata out of sync")
        FindStoreByNameResult.Found(metadata.toStoreMetadata())
    }
}
