package io.factstore.client.model

import java.time.Instant

data class Fact(
    val id: String,
    val type: String,
    val subject: String,
    val appendedAt: Instant,
    val payload: FactPayload,
    val metadata: Map<String, String>,
    val tags: Map<String, String>,
)

data class FactPayload(
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FactPayload) return false
        return data.contentEquals(other.data)
    }

    override fun hashCode(): Int = data.contentHashCode()
}
