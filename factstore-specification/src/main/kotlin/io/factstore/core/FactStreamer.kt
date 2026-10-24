package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * Streams the facts of a store, or the facts matching a [FactQuery], as a bounded [Flow].
 *
 * A stream follows this contract:
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
 * A stream starts where its [ReadDirection] begins, unless it is given a fact to continue after.
 * `continueAfter` is exclusive and follows the reading order: read forward it continues with the
 * facts appended after that one, read backward with those appended before it.
 *
 * For an unbounded stream that follows newly appended facts, see [FactSubscriber].
 *
 * @author Domenic Cassisi
 */
interface FactStreamer {

    /**
     * Streams the facts of a store: all of them, or those matching the request's query.
     *
     * A fact is emitted when it matches any of the query's items, and a fact matching several of
     * them is emitted once. Reading one subject, one type or a set of tags is a query of a single
     * item: the facts of subject `S` are `FactQuery(listOf(FactQueryItem(subjects = setOf(S))))`.
     *
     * @return [StreamFactsResult.FactStream], [StreamFactsResult.StoreNotFound] or
     *         [StreamFactsResult.ContinuationNotFound]
     */
    suspend fun streamFacts(request: StreamFactsRequest): StreamFactsResult

}

/**
 * Requests the facts of a store, or those matching a query.
 *
 * @property storeName the store to stream from
 * @property query the query a fact must match; `null` to stream every fact of the store
 * @property continueAfter the fact to continue after, exclusive and in reading order; `null` to
 *         start where the direction begins
 * @property direction the order in which facts are emitted
 * @property limit the maximum number of facts to emit, counted over the whole result
 */
data class StreamFactsRequest(
    val storeName: StoreName,
    val query: FactQuery? = null,
    val continueAfter: FactId? = null,
    val direction: ReadDirection,
    val limit: Limit = Limit.None,
)

/**
 * The outcome of [FactStreamer.streamFacts].
 */
sealed interface StreamFactsResult {

    /** The matching facts, read when collected; empty if nothing matches. */
    class FactStream(val facts: Flow<Fact>) : StreamFactsResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName) : StreamFactsResult

    /** The store holds no fact with the id the stream was to continue after. */
    data class ContinuationNotFound(val factId: FactId) : StreamFactsResult
}
