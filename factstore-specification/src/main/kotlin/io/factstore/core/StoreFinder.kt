package io.factstore.core

/**
 * Looks up the stores of a FactStore.
 */
interface StoreFinder {

    /** The metadata of every store. */
    suspend fun listStores(): List<StoreMetadata>

    /**
     * Checks whether a store of the requested name exists.
     *
     * @return [ExistsStoreByNameResult.Exists], or [ExistsStoreByNameResult.DoesNotExist]
     */
    suspend fun existsStoreByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult

    /**
     * Retrieves the metadata of the store of the requested name.
     *
     * @return [FindStoreByNameResult.Found], or [FindStoreByNameResult.NotFound]
     */
    suspend fun findStoreByName(request: FindStoreByNameRequest): FindStoreByNameResult

}

/**
 * Asks whether a store exists.
 *
 * @property storeName the name of the store
 */
data class ExistsStoreByNameRequest(
    val storeName: StoreName
)

/**
 * The outcome of [StoreFinder.existsStoreByName].
 */
sealed interface ExistsStoreByNameResult {

    /** A store of that name exists. */
    data object Exists : ExistsStoreByNameResult

    /** No store has that name. */
    data object DoesNotExist : ExistsStoreByNameResult

}

/**
 * Requests the metadata of a store.
 *
 * @property storeName the name of the store
 */
data class FindStoreByNameRequest(
    val storeName: StoreName
)

/**
 * The outcome of [StoreFinder.findStoreByName].
 */
sealed interface FindStoreByNameResult {

    /** The store exists, and this is its metadata. */
    data class Found(val storeMetadata: StoreMetadata) : FindStoreByNameResult

    /** No store has that name. */
    data class NotFound(val storeName: StoreName): FindStoreByNameResult

}
