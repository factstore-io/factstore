package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * Provides a streaming interface for consuming facts from the store.
 *
 * A [FactStreamer] allows clients to continuously read facts in order of
 * creation. Streaming is typically used for projections, read models,
 * integrations, or replaying facts for event-sourced systems.
 *
 * Implementations are expected to return facts in a deterministic order
 * and to respect the provided [StreamingOptions].
 *
 * @author Domenic Cassisi
 */
interface FactStreamer {

    /**
     * Streams all facts from the store according to the given streaming options.
     *
     * The returned [Flow] emits facts incrementally and may continue emitting
     * new facts as they are appended to the store.
     *
     * @param storeName the store from which to stream facts
     * @param streamingOptions configuration options controlling how facts
     * are streamed
     * @return a cold [Flow] emitting facts that match the streaming criteria,
     * or an error result if the fact store does not exist or the start position is invalid
     */
    suspend fun stream(storeName: StoreName, streamingOptions: StreamingOptions): StreamResult

    /**
     * Streams all facts from the beginning of the store.
     *
     * This is equivalent to calling [stream] with a default
     * [StreamingOptions].
     *
     * @return a cold [Flow] emitting all facts in order
     */
    suspend fun stream(storeName: StoreName) = stream(storeName, StreamingOptions())

}

/**
 * Start position options for streaming.
 */
sealed interface StartPosition {

    /**
     * Start streaming from the beginning of the store
     */
    data object Beginning : StartPosition

    /**
     * Start streaming from the end of the store
     */
    data object End : StartPosition

    /**
     * Start streaming after the given fact
     */
    @JvmInline
    value class After(val factId: FactId): StartPosition

}

/**
 * Configuration options controlling fact streaming behavior.
 *
 * @property startPosition the position from which to start streaming
 *
 * @author Domenic Cassisi
 */
data class StreamingOptions(
    val startPosition: StartPosition = StartPosition.Beginning,
)

sealed interface StreamResult {
    @JvmInline
    value class FactStream(val stream: Flow<Fact>) : StreamResult
    @JvmInline
    value class StoreNotFound(val storeName: StoreName) : StreamResult
    @JvmInline
    value class FactIdNotFound(val id: FactId) : StreamResult
}
