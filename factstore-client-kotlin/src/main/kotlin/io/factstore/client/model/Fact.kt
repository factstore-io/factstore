package io.factstore.client.model

import kotlinx.serialization.Serializable
import java.time.Instant

@Serializable
data class Fact(
    val id: String,
    val type: String,
    val subject: String,
    @Serializable(with = InstantSerializer::class)
    val appendedAt: Instant,
    val payload: FactPayload,
    val metadata: Map<String, String>,
    val tags: Map<String, String>,
)

@Serializable
data class FactPayload(
    @Serializable(with = ByteArrayAsBase64Serializer::class)
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FactPayload) return false
        return data.contentEquals(other.data)
    }

    override fun hashCode(): Int = data.contentHashCode()
}
