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
    val format: String? = null,
    val schemaRef: String? = null,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FactPayload) return false
        return data.contentEquals(other.data) && format == other.format && schemaRef == other.schemaRef
    }

    override fun hashCode(): Int {
        var result = data.contentHashCode()
        result = 31 * result + (format?.hashCode() ?: 0)
        result = 31 * result + (schemaRef?.hashCode() ?: 0)
        return result
    }
}
