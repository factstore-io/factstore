package io.factstore.core

/**
 * Provides read access to facts stored in the FactStore.
 *
 * Each operation accepts a dedicated request object that carries the store name,
 * query parameters, and optional pagination/ordering options. Results are typed
 * sealed interfaces that explicitly distinguish store-not-found from query-specific
 * outcomes.
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

    /**
     * Retrieves facts appended within a specific time window.
     *
     * @return [FindInTimeRangeResult.Found] or [FindInTimeRangeResult.StoreNotFound]
     */
    suspend fun findInTimeRange(request: FindInTimeRangeRequest): FindInTimeRangeResult

    /**
     * Retrieves the complete history of facts associated with a specific subject.
     *
     * @return [FindBySubjectResult.Found] or [FindBySubjectResult.StoreNotFound]
     */
    suspend fun findBySubject(request: FindBySubjectRequest): FindBySubjectResult

    /**
     * Finds facts that match the provided tags (AND logic).
     *
     * @return [FindByTagsResult.Found] or [FindByTagsResult.StoreNotFound]
     */
    suspend fun findByTags(request: FindByTagsRequest): FindByTagsResult

    /**
     * Performs an expressive search for facts using a structured tag query.
     *
     * @return [FindByTagQueryResult.Found] or [FindByTagQueryResult.StoreNotFound]
     */
    suspend fun findByTagQuery(request: FindByTagQueryRequest): FindByTagQueryResult
}
