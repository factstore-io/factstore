package io.factstore.core

/**
 * Provides read access to facts stored in the FactStore.
 *
 * Implementations are responsible for retrieving facts according to the
 * specified criteria while preserving the append-only nature and ordering
 * semantics defined by the store.
 *
 * @author Domenic Cassisi
 */
interface FactFinder {

    /**
     * Retrieves a specific fact by its unique identifier within a named store.
     *
     * This method is the primary way to access a single event when the [FactId] is known.
     * Because store resolution and fact lookup are distinct operations, the result
     * explicitly distinguishes between a missing store and a missing fact.
     *
     * @param storeName the logical name of the store
     * @param factId the unique identifier of the fact to retrieve.
     * @return a [FindByIdResult] which can be handled using a `when` expression:
     * - [FindByIdResult.Found]: contains the requested [Fact].
     * - [FindByIdResult.NotFound]: the store exists, but the fact ID is unknown.
     * - [FindByIdResult.StoreNotFound]: the provided [storeName] does not exist.
     */
    suspend fun findById(storeName: StoreName, factId: FactId): FindByIdResult

    /**
     * Checks for the existence of a specific fact within a named store.
     *
     * Use this lightweight check when you only need to verify presence without
     * fetching the full fact payload.
     *
     * @param storeName the logical name of the store.
     * @param factId the unique identifier of the fact.
     * @return an [ExistsByIdResult] indicating if the fact was [ExistsByIdResult.Exists],
     *         [ExistsByIdResult.DoesNotExist], or if the [ExistsByIdResult.StoreNotFound].
     */
    suspend fun existsById(storeName: StoreName, factId: FactId): ExistsByIdResult

    /**
     * Retrieves a chronological list of facts appended within a specific time window.
     *
     * This method is useful for querying recent events or performing time-based analyses.
     *
     * @param storeName the logical name of the store.
     * @param timeRange the inclusive temporal boundaries for the search.
     * @return a [FindInTimeRangeResult] containing the list of facts, or
     *         [FindInTimeRangeResult.StoreNotFound] if the store does not exist.
     */
    suspend fun findInTimeRange(storeName: StoreName, timeRange: TimeRange): FindInTimeRangeResult

    /**
     * Retrieves the complete history of facts associated with a specific subject.
     *
     * @param storeName the logical name of the store.
     * @param subjectRef the subject reference
     * @return a [FindBySubjectResult.Found] containing the stream of facts for this subject,
     *         or [FindBySubjectResult.StoreNotFound] if the store is missing.
     */
    suspend fun findBySubject(storeName: StoreName, subjectRef: SubjectRef): FindBySubjectResult

    /**
     * Finds facts that match at least one of the provided tags (OR logic).
     *
     * @param storeName the logical name of the store.
     * @param tags a list of key-value pairs to match against.
     * @return a [FindByTagsResult.Found] with matching facts, or
     *         [FindByTagsResult.StoreNotFound] if the store does not exist.
     */
    suspend fun findByTags(storeName: StoreName, tags: List<Pair<TagKey, TagValue>>): FindByTagsResult

    /**
     * Performs an expressive search for facts using a structured tag query.
     *
     * Tag queries allow more expressive matching semantics than simple
     * key-value pairs.
     *
     * @param storeName the logical name of the store.
     * @param query the tag query
     * @return a [FindByTagQueryResult.Found] containing the filtered facts, or
     *         [FindByTagQueryResult.StoreNotFound] if the store name cannot be resolved.
     */
    suspend fun findByTagQuery(storeName: StoreName, query: TagQuery): FindByTagQueryResult
}
