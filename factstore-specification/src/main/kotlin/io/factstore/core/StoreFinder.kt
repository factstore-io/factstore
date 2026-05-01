package io.factstore.core

interface StoreFinder {

    suspend fun listAll(): List<StoreMetadata>

    suspend fun existsByName(name: StoreName): Boolean

    suspend fun findByName(name: StoreName): StoreMetadata?

}
