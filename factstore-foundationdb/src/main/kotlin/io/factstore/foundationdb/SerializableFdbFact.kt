package io.factstore.foundationdb

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.util.*

@Serializable
data class SerializableFdbFact(
    @Contextual
    val id: UUID,
    val type: String,
    val subject: String,
    val timeEpochSeconds: Long,
    val timeNanos: Int,
    val payload: ByteArray,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SerializableFdbFact) return false

        return id == other.id &&
                type == other.type &&
                subject == other.subject &&
                timeEpochSeconds == other.timeEpochSeconds &&
                timeNanos == other.timeNanos &&
                payload.contentEquals(other.payload) &&
                metadata == other.metadata &&
                tags == other.tags
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + subject.hashCode()
        result = 31 * result + timeEpochSeconds.hashCode()
        result = 31 * result + timeNanos
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + tags.hashCode()
        return result
    }
}
