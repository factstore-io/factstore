package io.factstore.core

fun interface StoreRemover {

    suspend fun remove(request: RemoveStoreRequest): RemoveStoreResult

}

data class RemoveStoreRequest(
    val storeName: StoreName
)

sealed interface RemoveStoreResult {

    data class StoreRemoved(val storeName: StoreName) : RemoveStoreResult
    data class StoreNotFound(val storeName: StoreName) : RemoveStoreResult

}
