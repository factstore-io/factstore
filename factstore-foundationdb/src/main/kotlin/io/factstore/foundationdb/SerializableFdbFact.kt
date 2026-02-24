package io.factstore.foundationdb

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
    val payload: SerializableFactPayload,
)

@Serializable
data class SerializableFactPayload(
    val data: ByteArray,
    val format: String?,
    val schema: String?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SerializableFactPayload

        if (!data.contentEquals(other.data)) return false
        if (format != other.format) return false
        if (schema != other.schema) return false

        return true
    }

    override fun hashCode(): Int {
        var result = data.contentHashCode()
        result = 31 * result + format.hashCode()
        result = 31 * result + schema.hashCode()
        return result
    }
}
