package io.factstore.core

sealed interface FindStoreByNameResult {

    data class Found(val storeMetadata: StoreMetadata) : FindStoreByNameResult
    data class NotFound(val storeName: StoreName): FindStoreByNameResult

}
