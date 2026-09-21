package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * Streams the facts of a store, or a selection of them, as a bounded [Flow].
 *
 * Every operation follows the same contract:
 *
 * - **Pinned at call time.** The call resolves the store and its current head in
 *   one consistent read. The stream emits the matching facts up to and including
 *   that head; facts appended afterwards are excluded.
 * - **Cold and repeatable.** Nothing is read until the stream is collected, and
 *   every collection emits the same facts.
 * - **Ordered.** Facts are emitted in store order: oldest first for
 *   [ReadDirection.Forward], newest first for [ReadDirection.Backward].
 * - **Independent of the consumer's pace.** A stream is not subject to the time or
 *   size limits of a storage transaction, however slowly it is collected.
 *   Cancelling the collection stops the reading.
 *
 *
 * For an unbounded stream that follows newly appended facts, see [FactSubscriber].
 *
 * @author Domenic Cassisi
 */
interface FactStreamer {

    /**
     * Streams all facts of a store.
     *
     * @return [StreamFactsResult.FactStream] or [StreamFactsResult.StoreNotFound]
     */
    suspend fun streamFacts(request: StreamFactsRequest): StreamFactsResult

    /**
     * Streams the facts of a single subject.
     *
     * @return [StreamFactsBySubjectResult.FactStream] or [StreamFactsBySubjectResult.StoreNotFound]
     */
    suspend fun streamFactsBySubject(request: StreamFactsBySubjectRequest): StreamFactsBySubjectResult

    /**
     * Streams the facts of a single type.
     *
     * The type is matched exactly: `com.acme.OrderPlaced` is not matched by `com.acme`.
     *
     * @return [StreamFactsByTypeResult.FactStream] or [StreamFactsByTypeResult.StoreNotFound]
     */
    suspend fun streamFactsByType(request: StreamFactsByTypeRequest): StreamFactsByTypeResult

    /**
     * Streams the facts that carry all of the requested tags.
     *
     * A fact matches when it carries every tag with exactly the requested value, so the tags are
     * combined with AND. A tag key can therefore be required only once: "region is eu or us" is
     * not a tag stream but a query.
     *
     * @return [StreamFactsByTagsResult.FactStream] or [StreamFactsByTagsResult.StoreNotFound]
     */
    suspend fun streamFactsByTags(request: StreamFactsByTagsRequest): StreamFactsByTagsResult

    /**
     * Streams the facts matching a [FactQuery].
     *
     * The general read: a fact is emitted when it matches any of the query's filters, and a fact
     * matching several of them is emitted once. The other operations are its shorthands, and each
     * equals a query of a single filter — streaming by subject `S` matches
     * `FactQuery(listOf(FactFilter(subjects = setOf(S))))`.
     *
     * @return [StreamFactsByQueryResult.FactStream] or [StreamFactsByQueryResult.StoreNotFound]
     */
    suspend fun streamFactsByQuery(request: StreamFactsByQueryRequest): StreamFactsByQueryResult

}

/**
 * Requests all facts of a store.
 *
 * @property storeName the store to stream from
 * @property direction the order in which facts are emitted
 * @property limit the maximum number of facts to emit
 */
data class StreamFactsRequest(
    val storeName: StoreName,
    val direction: ReadDirection,
    val limit: Limit,
)

/**
 * The outcome of [FactStreamer.streamFacts].
 */
sealed interface StreamFactsResult {

    /** The store's facts, read when collected; empty if the store has none. */
    class FactStream(val facts: Flow<Fact>) : StreamFactsResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName) : StreamFactsResult
}

/**
 * Requests the facts of a single subject.
 *
 * @property storeName the store to stream from
 * @property subject the subject whose facts are emitted
 * @property direction the order in which facts are emitted
 * @property limit the maximum number of facts to emit
 */
data class StreamFactsBySubjectRequest(
    val storeName: StoreName,
    val subject: Subject,
    val direction: ReadDirection,
    val limit: Limit,
)

/**
 * The outcome of [FactStreamer.streamFactsBySubject].
 */
sealed interface StreamFactsBySubjectResult {

    /** The subject's facts, read when collected; empty if the subject has none. */
    class FactStream(val facts: Flow<Fact>) : StreamFactsBySubjectResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName) : StreamFactsBySubjectResult
}


/**
 * Requests the facts of a single type.
 *
 * @property storeName the store to stream from
 * @property type the type whose facts are emitted, matched exactly
 * @property direction the order in which facts are emitted
 * @property limit the maximum number of facts to emit
 */
data class StreamFactsByTypeRequest(
    val storeName: StoreName,
    val type: FactType,
    val direction: ReadDirection,
    val limit: Limit,
)

/**
 * The outcome of [FactStreamer.streamFactsByType].
 */
sealed interface StreamFactsByTypeResult {

    /** The type's facts, read when collected; empty if no fact has the type. */
    class FactStream(val facts: Flow<Fact>) : StreamFactsByTypeResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName) : StreamFactsByTypeResult
}

/**
 * Requests the facts that carry all of the given tags.
 *
 * @property storeName the store to stream from
 * @property tags the tags a fact must carry, all of them; at least one, and at most
 *         [FactInput.MAX_TAGS], since no fact carries more
 * @property direction the order in which facts are emitted
 * @property limit the maximum number of facts to emit
 *
 * @throws IllegalArgumentException if [tags] is empty, or requires more tags than a fact can carry
 */
data class StreamFactsByTagsRequest(
    val storeName: StoreName,
    val tags: Map<TagKey, TagValue>,
    val direction: ReadDirection,
    val limit: Limit,
) {
    init {
        require(tags.isNotEmpty()) { "Tags must be defined!" }
        requireSatisfiableTagCount(tags)
    }
}

/**
 * The outcome of [FactStreamer.streamFactsByTags].
 */
sealed interface StreamFactsByTagsResult {

    /** The facts carrying the tags, read when collected; empty if no fact carries them all. */
    class FactStream(val facts: Flow<Fact>) : StreamFactsByTagsResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName) : StreamFactsByTagsResult
}

/**
 * Requests the facts matching a query.
 *
 * @property storeName the store to stream from
 * @property query the query a fact must match
 * @property direction the order in which facts are emitted
 * @property limit the maximum number of facts to emit, counted over the whole result
 */
data class StreamFactsByQueryRequest(
    val storeName: StoreName,
    val query: FactQuery,
    val direction: ReadDirection,
    val limit: Limit,
)

/**
 * The outcome of [FactStreamer.streamFactsByQuery].
 */
sealed interface StreamFactsByQueryResult {

    /** The matching facts, read when collected; empty if the query matches nothing. */
    class FactStream(val facts: Flow<Fact>) : StreamFactsByQueryResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName) : StreamFactsByQueryResult
}
