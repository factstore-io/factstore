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
