package org.factstore.core

import kotlinx.coroutines.flow.Flow

interface FactStreamer {

    fun streamAll(streamingOptionSet: StreamingOptionSet): Flow<Fact>

    fun streamAll() = streamAll(StreamingOptionSet())

}

data class StreamingOptionSet(
    val lastSeenId: FactId? = null,
    val batchSize: Int = 128,
    val pollDelayMs: Long = 250L
)