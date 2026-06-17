package io.factstore.core

fun interface StoreRemover {

    suspend fun remove(request: RemoveStoreRequest): RemoveStoreResult

}
