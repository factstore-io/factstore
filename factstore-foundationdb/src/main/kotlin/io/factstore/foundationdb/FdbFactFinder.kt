package io.factstore.foundationdb

import io.factstore.core.*

internal class FdbFactFinder(storage: FdbStorage) : FactFinder, FdbAccess by storage {

    override suspend fun findFactById(request: FindFactByIdRequest): FindFactByIdResult = inReadTransaction {
        val storeId = storeIdOf(request.storeName)
            ?: return@inReadTransaction FindFactByIdResult.StoreNotFound(request.storeName)

        loadFact(storeId, request.factId)?.let(FindFactByIdResult::Found) ?: FindFactByIdResult.NotFound(request.factId)
    }

    override suspend fun existsFactById(request: ExistsFactByIdRequest): ExistsFactByIdResult = inReadTransaction {
        val storeId = storeIdOf(request.storeName)
            ?: return@inReadTransaction ExistsFactByIdResult.StoreNotFound(request.storeName)

        if (positionOf(storeId, request.factId) != null) ExistsFactByIdResult.Exists else ExistsFactByIdResult.DoesNotExist
    }

}
