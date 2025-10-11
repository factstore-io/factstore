package com.cassisi.openeventstore.core.dcb

import kotlinx.coroutines.flow.Flow
import java.util.UUID

interface FactStreamer {

    fun streamAll(streamingOptionSet: StreamingOptionSet): Flow<Fact>

    fun streamAll() = streamAll(StreamingOptionSet())

}

data class StreamingOptionSet( // find better name
    val lastSeenId: UUID? = null, // todo later
    val batchSize: Int = 128,
    val pollDelayMs: Long = 250L
)