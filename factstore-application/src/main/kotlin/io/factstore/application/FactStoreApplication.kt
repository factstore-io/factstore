package io.factstore.application

import io.factstore.application.slices.CreateStoreSlice
import io.factstore.core.FactId
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.domain.CreateStore
import io.factstore.domain.SystemFact

class FactStoreApplication(
    private val createStoreSlice: CreateStoreSlice,
    private val storeIdGenerator: StoreIdGenerator = StoreIdGenerator(StoreId::generate),
) {

    suspend fun createStore(storeName: StoreName, idempotency: Idempotency): DecisionResult<SystemFact, FactId> =
        createStoreSlice.handle(CreateStore(storeIdGenerator.generate(), storeName), idempotency)
}
