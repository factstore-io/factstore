package io.factstore.core

interface StoreFinder {

    suspend fun listAll(): List<StoreMetadata>

    suspend fun existsByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult

    suspend fun findByName(request: FindStoreByNameRequest): FindStoreByNameResult

}

data class ExistsStoreByNameRequest(
    val storeName: StoreName
)

sealed interface ExistsStoreByNameResult {

    data object StoreExists : ExistsStoreByNameResult
    data object StoreAbsent : ExistsStoreByNameResult

}

data class FindStoreByNameRequest(
    val storeName: StoreName
)

sealed interface FindStoreByNameResult {

    data class Found(val storeMetadata: StoreMetadata) : FindStoreByNameResult
    data class NotFound(val storeName: StoreName): FindStoreByNameResult

}
