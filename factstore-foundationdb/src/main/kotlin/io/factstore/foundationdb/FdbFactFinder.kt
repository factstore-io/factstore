package io.factstore.foundationdb

import io.factstore.core.*

internal class FdbFactFinder(storage: FdbStorage) : FactFinder, FdbAccess by storage {

    override suspend fun findById(request: FindByIdRequest): FindByIdResult = inReadTransaction {
        val storeId = storeIdOf(request.storeName)
            ?: return@inReadTransaction FindByIdResult.StoreNotFound(request.storeName)

        loadFact(storeId, request.factId)?.let(FindByIdResult::Found) ?: FindByIdResult.NotFound(request.factId)
    }

    override suspend fun existsById(request: ExistsByIdRequest): ExistsByIdResult = inReadTransaction {
        val storeId = storeIdOf(request.storeName)
            ?: return@inReadTransaction ExistsByIdResult.StoreNotFound(request.storeName)

        if (positionOf(storeId, request.factId) != null) ExistsByIdResult.Exists else ExistsByIdResult.DoesNotExist
    }

}
