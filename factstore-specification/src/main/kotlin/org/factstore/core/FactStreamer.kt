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
 * and to respect the provided [StreamingOptionSet].
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
     * If [StreamingOptionSet.lastSeenId] is set and does not reference an
     * existing fact, an [InvalidStreamingRequestException] is thrown.
     *
     * @param streamingOptionSet configuration options controlling how facts
     * are streamed
     * @return a cold [Flow] emitting facts that match the streaming criteria
     *
     * @throws InvalidStreamingRequestException if the streaming request
     * cannot be satisfied
     */
    fun streamAll(streamingOptionSet: StreamingOptionSet): Flow<Fact>

    /**
     * Streams all facts from the beginning of the store using default options.
     *
     * This is equivalent to calling [streamAll] with a default
     * [StreamingOptionSet].
     *
     * @return a cold [Flow] emitting all facts in order
     */
    fun streamAll() = streamAll(StreamingOptionSet())

}

/**
 * Configuration options controlling fact streaming behavior.
 *
 * These options allow clients to resume streaming from a known position,
 * control batching behavior, and tune polling frequency when no new facts
 * are available.
 *
 * @property lastSeenId the identifier of the last processed fact; streaming
 * will resume with the next fact after this identifier.
 * If the identifier does not exist, the streaming request is considered invalid.
 * @property batchSize the maximum number of facts fetched per polling cycle
 * @property pollDelayMs the delay in milliseconds between polling attempts
 * when no new facts are available
 *
 * @author Domenic Cassisi
 */
data class StreamingOptionSet(
    val lastSeenId: FactId? = null,
    val batchSize: Int = 128,
    val pollDelayMs: Long = 250L
) {

    init {
        require(batchSize > 0) { "Batch size must be greater than zero, but was $batchSize" }
        require(pollDelayMs > 0) { "Poll delay must be greater than zero, but was $pollDelayMs" }
    }

}
