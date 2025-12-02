package org.factstore.core

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.util.*

@Serializable
data class SerializableFdbFact(
    @Contextual
    val id: UUID,
    val type: String,
    val subjectType: String,
    val subjectId: String,
    val timeEpochSeconds: Long,
    val timeNanos: Int,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
    val payload: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SerializableFdbFact

        if (id != other.id) return false
        if (type != other.type) return false
        if (subjectType != other.subjectType) return false
        if (subjectId != other.subjectId) return false
        if (timeEpochSeconds != other.timeEpochSeconds) return false
        if (timeNanos != other.timeNanos) return false
        if (metadata != other.metadata) return false
        if (tags != other.tags) return false
        if (!payload.contentEquals(other.payload)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + subjectType.hashCode()
        result = 31 * result + subjectId.hashCode()
        result = 31 * result + timeEpochSeconds.hashCode()
        result = 31 * result + timeNanos.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + tags.hashCode()
        result = 31 * result + payload.contentHashCode()
        return result
    }
}
