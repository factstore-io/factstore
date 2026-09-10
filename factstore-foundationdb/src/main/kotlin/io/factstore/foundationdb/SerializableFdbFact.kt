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
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
    val payload: SerializableFactPayload,
)

@Serializable
data class SerializableFactPayload(
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SerializableFactPayload

        return data.contentEquals(other.data)
    }

    override fun hashCode(): Int = data.contentHashCode()
}
