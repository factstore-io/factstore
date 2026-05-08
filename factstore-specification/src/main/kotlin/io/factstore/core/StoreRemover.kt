package io.factstore.core

fun interface StoreRemover {

    suspend fun handle(request: RemoveStoreRequest): RemoveStoreResult

}
