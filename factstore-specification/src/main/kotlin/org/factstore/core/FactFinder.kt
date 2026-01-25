package org.factstore.core

import java.time.Instant

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
    suspend fun findById(factId: FactId): Fact?

    /**
     * Checks whether a fact with the given identifier exists.
     *
     * @param factId the identifier of the fact
     * @return `true` if a fact with the given identifier exists, `false` otherwise
     */
    suspend fun existsById(factId: FactId): Boolean

    /**
     * Finds all facts created within the given time range.
     *
     * The range is inclusive of both start and end timestamps.
     *
     * @param start the start of the time range
     * @param end the end of the time range (defaults to the current time)
     * @return the list of facts created within the specified time range
     */
    suspend fun findInTimeRange(start: Instant, end: Instant = Instant.now()): List<Fact>

    /**
     * Finds all facts associated with the given subject.
     *
     * @param subjectRef the subject reference
     * @return the list of facts associated with the subject
     */
    suspend fun findBySubject(subjectRef: SubjectRef): List<Fact>

    /**
     * Finds all facts that match the given set of tags.
     *
     * The provided tags are interpreted as an OR match requirement.
     *
     * @param tags the list of tag key-value pairs
     * @return the list of facts matching the specified tags
     */
    suspend fun findByTags(tags: List<Pair<TagKey, TagValue>>): List<Fact>

    /**
     * Finds all facts that match the given tag query.
     *
     * Tag queries allow more expressive matching semantics than simple
     * key-value pairs.
     *
     * @param query the tag query
     * @return the list of facts matching the query
     */
    suspend fun findByTagQuery(query: TagQuery): List<Fact>

}
