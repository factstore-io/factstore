package io.factstore.core

sealed interface RemoveStoreResult {

    data class StoreRemoved(val storeName: StoreName) : RemoveStoreResult
    data class StoreNotFound(val storeName: StoreName) : RemoveStoreResult

}
