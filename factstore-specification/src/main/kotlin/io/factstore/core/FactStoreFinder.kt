package io.factstore.core

interface FactStoreFinder {

    suspend fun listAll(): List<FactStoreMetadata>

    suspend fun existsByName(name: FactStoreName): Boolean

}
