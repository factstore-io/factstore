package io.factstore.core

fun interface StoreRemover {

    suspend fun removeStore(request: RemoveStoreRequest): RemoveStoreResult

}

data class RemoveStoreRequest(
    val storeName: StoreName
)

sealed interface RemoveStoreResult {

    data class Removed(val storeName: StoreName) : RemoveStoreResult
    data class NotFound(val storeName: StoreName) : RemoveStoreResult

}
