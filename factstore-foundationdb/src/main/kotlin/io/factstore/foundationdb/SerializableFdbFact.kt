package io.factstore.foundationdb

import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.*
import kotlinx.serialization.Contextual
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import java.time.Instant
import kotlinx.serialization.Serializable
import java.util.UUID

@Serializable
internal data class SerializableFdbFact(
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

internal fun Fact.toSerializableFdbFact() = SerializableFdbFact(
    id = id.uuid,
    type = type.value,
    subject = subject.value,
    timeEpochSeconds = appendedAt.epochSecond,
    timeNanos = appendedAt.nano,
    metadata = metadata.entries.associate { it.key.value to it.value.value },
    tags = tags.entries.associate { it.key.value to it.value.value },
    payload = payload.data,
)

internal fun SerializableFdbFact.encodeToByteArray() = Avro.encodeToByteArray(this)

internal fun ByteArray.toSerializableFdbFact() = Avro.decodeFromByteArray<SerializableFdbFact>(this)

internal fun SerializableFdbFact.toFact() = Fact(
    id = FactId(id),
    type = FactType(type),
    payload = FactPayload(data = payload),
    subject = Subject(subject),
    appendedAt = Instant.ofEpochSecond(timeEpochSeconds, timeNanos.toLong()),
    metadata = metadata.entries.associate { it.key.toMetadataKey() to it.value.toMetadataValue() },
    tags = tags.entries.associate { it.key.toTagKey() to it.value.toTagValue() }
)
