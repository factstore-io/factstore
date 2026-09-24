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
     * @return [FindByIdResult.Found], [FindByIdResult.NotFound], or [FindByIdResult.StoreNotFound]
     */
    suspend fun findById(request: FindByIdRequest): FindByIdResult

    /**
     * Checks for the existence of a specific fact within a named store.
     *
     * @return [ExistsByIdResult.Exists], [ExistsByIdResult.DoesNotExist], or [ExistsByIdResult.StoreNotFound]
     */
    suspend fun existsById(request: ExistsByIdRequest): ExistsByIdResult
}
