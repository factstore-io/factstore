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

/**
 * Requests one fact of a store.
 *
 * @property storeName the store holding the fact
 * @property factId the id of the fact
 */
data class FindFactByIdRequest(
    val storeName: StoreName,
    val factId: FactId,
)

/**
 * The outcome of [FactFinder.findFactById].
 */
sealed interface FindFactByIdResult {

    /** The store holds the fact. */
    data class Found(val fact: Fact): FindFactByIdResult

    /** The store holds no fact with that id. */
    data class NotFound(val factId: FactId): FindFactByIdResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName): FindFactByIdResult
}

/**
 * Asks whether a store holds a fact.
 *
 * @property storeName the store to look in
 * @property factId the id of the fact
 */
data class ExistsFactByIdRequest(
    val storeName: StoreName,
    val factId: FactId,
)

/**
 * The outcome of [FactFinder.existsFactById].
 */
sealed interface ExistsFactByIdResult {

    /** The store holds the fact. */
    data object Exists: ExistsFactByIdResult

    /** The store holds no fact with that id. */
    data object DoesNotExist: ExistsFactByIdResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName): ExistsFactByIdResult
}
