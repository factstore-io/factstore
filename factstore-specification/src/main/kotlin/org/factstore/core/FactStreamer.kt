package org.factstore.core

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
     * If [StreamingOptions.startPosition] is set and does not reference an
     * existing fact, an [InvalidStreamingRequestException] is thrown.
     *
     * @param streamingOptions configuration options controlling how facts
     * are streamed
     * @return a cold [Flow] emitting facts that match the streaming criteria
     *
     * @throws InvalidStreamingRequestException if the streaming request
     * cannot be satisfied
     */
    fun stream(streamingOptions: StreamingOptions): Flow<Fact>

    /**
     * Streams all facts from the beginning of the store.
     *
     * This is equivalent to calling [stream] with a default
     * [StreamingOptions].
     *
     * @return a cold [Flow] emitting all facts in order
     */
    fun stream() = stream(StreamingOptions())

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
