package io.factstore.foundationdb

import io.factstore.core.*

class FdbFactFinder(private val fdbFactStore: FdbFactStore) : FactFinder {

    private val db = fdbFactStore.db

    private val context = fdbFactStore.context

    override suspend fun findById(request: FindByIdRequest): FindByIdResult = db.inReadTransaction {
        val storeId = context.lookUpStoreIdByName(request.storeName)
            ?: return@inReadTransaction FindByIdResult.StoreNotFound(request.storeName)

        val fact = with(fdbFactStore) { with(storeId) { request.factId.loadFactById() } }
        if (fact != null) FindByIdResult.Found(fact.fact) else FindByIdResult.NotFound(request.factId)
    }

    override suspend fun existsById(request: ExistsByIdRequest): ExistsByIdResult = db.inReadTransaction {
        val storeId = context.lookUpStoreIdByName(request.storeName)
            ?: return@inReadTransaction ExistsByIdResult.StoreNotFound(request.storeName)

        if (context.factPositionIndexSubspace.exists(storeId, request.factId)) ExistsByIdResult.Exists
        else ExistsByIdResult.DoesNotExist
    }

}
