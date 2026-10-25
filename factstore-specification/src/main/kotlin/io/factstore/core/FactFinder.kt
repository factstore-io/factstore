package io.factstore.core

/**
 * Provides read access to facts stored in the FactStore.
 *
 * Reads that select several facts belong to [FactStreamer], which streams them; what
 * remains here are the lookups of a single fact. Each operation accepts a dedicated
 * request object, and results are typed sealed interfaces that explicitly distinguish
 * store-not-found from the operation's own outcomes.
 *
 * @author Domenic Cassisi
 */
interface FactFinder {

    /**
     * Retrieves a specific fact by its unique identifier within a named store.
     *
     * @return [FindFactByIdResult.Found], [FindFactByIdResult.NotFound], or [FindFactByIdResult.StoreNotFound]
     */
    suspend fun findFactById(request: FindFactByIdRequest): FindFactByIdResult

    /**
     * Checks for the existence of a specific fact within a named store.
     *
     * @return [ExistsFactByIdResult.Exists], [ExistsFactByIdResult.DoesNotExist], or [ExistsFactByIdResult.StoreNotFound]
     */
    suspend fun existsFactById(request: ExistsFactByIdRequest): ExistsFactByIdResult
}

data class FindFactByIdRequest(
    val storeName: StoreName,
    val factId: FactId,
)

sealed interface FindFactByIdResult {
    data class Found(val fact: Fact): FindFactByIdResult
    data class NotFound(val factId: FactId): FindFactByIdResult
    data class StoreNotFound(val storeName: StoreName): FindFactByIdResult
}

data class ExistsFactByIdRequest(
    val storeName: StoreName,
    val factId: FactId,
)

sealed interface ExistsFactByIdResult {
    data object Exists: ExistsFactByIdResult
    data object DoesNotExist: ExistsFactByIdResult
    data class StoreNotFound(val storeName: StoreName): ExistsFactByIdResult
}
