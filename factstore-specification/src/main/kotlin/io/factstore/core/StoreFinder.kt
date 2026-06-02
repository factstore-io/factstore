package io.factstore.core

interface StoreFinder {

    suspend fun listAll(): List<StoreMetadata>

    suspend fun existsByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult

    suspend fun findByName(request: FindStoreByNameRequest): FindStoreByNameResult

}
