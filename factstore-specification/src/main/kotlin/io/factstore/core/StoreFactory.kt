package io.factstore.core

fun interface StoreFactory {

    suspend fun createStore(request: CreateStoreRequest): CreateStoreResult

}

data class CreateStoreRequest(
    val storeName: StoreName,
)

sealed interface CreateStoreResult {

    data class Created(val id: StoreId) : CreateStoreResult
    data class NameAlreadyExists(val storeName: StoreName) : CreateStoreResult

}
