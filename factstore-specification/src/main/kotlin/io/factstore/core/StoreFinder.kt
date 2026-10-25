package io.factstore.core

interface StoreFinder {

    suspend fun listStores(): List<StoreMetadata>

    suspend fun existsStoreByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult

    suspend fun findStoreByName(request: FindStoreByNameRequest): FindStoreByNameResult

}

data class ExistsStoreByNameRequest(
    val storeName: StoreName
)

sealed interface ExistsStoreByNameResult {

    data object Exists : ExistsStoreByNameResult
    data object DoesNotExist : ExistsStoreByNameResult

}

data class FindStoreByNameRequest(
    val storeName: StoreName
)

sealed interface FindStoreByNameResult {

    data class Found(val storeMetadata: StoreMetadata) : FindStoreByNameResult
    data class NotFound(val storeName: StoreName): FindStoreByNameResult

}
