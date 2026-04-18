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
     * Finds a fact by its unique identifier.
     *
     * @param factId the identifier of the fact
     * @return the fact if it exists, or `null` otherwise
     */
    suspend fun findById(factStoreId: FactStoreId, factId: FactId): FindByIdResult

    /**
     * Checks whether a fact with the given identifier exists.
     *
     * @param factId the identifier of the fact
     * @return `true` if a fact with the given identifier exists, `false` otherwise
     */
    suspend fun existsById(factStoreId: FactStoreId, factId: FactId): ExistsByIdResult

    /**
     * Finds all facts created within the given time range.
     *
     * The range is inclusive of both start and end timestamps.
     *
     * @param timeRange the time range to cover
     * @return the list of facts created within the specified time range, or an error if the factstore doesn't exist
     */
    suspend fun findInTimeRange(factStoreId: FactStoreId, timeRange: TimeRange): FindInTimeRangeResult

    /**
     * Finds all facts associated with the given subject.
     *
     * @param subjectRef the subject reference
     * @return the list of facts associated with the subject, or an error if the factstore doesn't exist
     */
    suspend fun findBySubject(factStoreId: FactStoreId, subjectRef: SubjectRef): FindBySubjectResult

    /**
     * Finds all facts that match the given set of tags.
     *
     * The provided tags are interpreted as an OR match requirement.
     *
     * @param tags the list of tag key-value pairs
     * @return the list of facts matching the specified tags, or an error if the factstore doesn't exist
     */
    suspend fun findByTags(factStoreId: FactStoreId, tags: List<Pair<TagKey, TagValue>>): FindByTagsResult

    /**
     * Finds all facts that match the given tag query.
     *
     * Tag queries allow more expressive matching semantics than simple
     * key-value pairs.
     *
     * @param query the tag query
     * @return the list of facts matching the query, or an error if the factstore doesn't exist
     */
    suspend fun findByTagQuery(factStoreId: FactStoreId, query: TagQuery): FindByTagQueryResult
}
