package io.factstore.core

interface FactStoreFactory {

    suspend fun handle(request: CreateFactStoreRequest): CreateFactStoreResult

}
