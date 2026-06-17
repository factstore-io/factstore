package io.factstore.core

fun interface StoreFactory {

    suspend fun create(request: CreateStoreRequest): CreateStoreResult

}
