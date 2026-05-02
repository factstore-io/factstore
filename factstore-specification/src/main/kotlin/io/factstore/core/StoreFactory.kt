package io.factstore.core

fun interface StoreFactory {

    suspend fun handle(request: CreateStoreRequest): CreateStoreResult

}
