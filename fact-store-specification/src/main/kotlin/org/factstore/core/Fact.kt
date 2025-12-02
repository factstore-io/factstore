package org.factstore.core

import java.time.Instant
import java.util.*

data class Fact(
    val id: FactId,
    val type: String,
    val payload: ByteArray,
    val subject: Subject,
    val createdAt: Instant,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Fact

        if (id != other.id) return false
        if (type != other.type) return false
        if (!payload.contentEquals(other.payload)) return false
        if (subject != other.subject) return false
        if (createdAt != other.createdAt) return false
        if (metadata != other.metadata) return false
        if (tags != other.tags) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + subject.hashCode()
        result = 31 * result + createdAt.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + tags.hashCode()
        return result
    }
}

data class Subject(
    val type: String,
    val id: String
)

@JvmInline
value class FactId(val uuid: UUID) {

    companion object {

        fun generate() = FactId(UUID.randomUUID())

    }
}

fun UUID.toFactId() = FactId(this)
