package io.factstore.core

sealed interface CreateStoreResult {

    data class Created(val id: StoreId) : CreateStoreResult
    data class NameAlreadyExists(val storeName: StoreName) : CreateStoreResult

}
