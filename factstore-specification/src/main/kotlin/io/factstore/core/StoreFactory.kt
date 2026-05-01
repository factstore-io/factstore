package io.factstore.core

interface StoreFactory {

    suspend fun handle(request: CreateStoreRequest): CreateStoreResult

}
