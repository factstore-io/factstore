package io.factstore.client.internal

import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.factstore.client.model.AppendCondition
import io.factstore.client.model.Fact
import io.factstore.client.model.FactInput
import io.factstore.client.model.FactPayload
import io.factstore.client.model.ReadDirection
import io.factstore.client.model.ServerInfo
import io.factstore.client.model.StoreInfo
import io.factstore.client.model.TagQuery
import io.factstore.client.model.TagQueryItem
import io.factstore.grpc.v1.FactStoreProto
import io.factstore.grpc.v1.all
import io.factstore.grpc.v1.appendCondition
import io.factstore.grpc.v1.expectedLastFact
import io.factstore.grpc.v1.factInput
import io.factstore.grpc.v1.factPayload
import io.factstore.grpc.v1.tagOnlyItem
import io.factstore.grpc.v1.tagQuery
import io.factstore.grpc.v1.tagQueryBasedCondition
import io.factstore.grpc.v1.tagQueryItem
import io.factstore.grpc.v1.tagTypeItem
import java.time.Instant

// ─── Proto → Domain ───────────────────────────────────────────────────────────

internal fun FactStoreProto.Fact.toDomain(): Fact = Fact(
    id = id,
    type = type,
    subject = subject,
    appendedAt = appendedAt.toInstant(),
    payload = payload.toDomain(),
    metadata = metadataMap.toMap(),
    tags = tagsMap.toMap(),
)

internal fun FactStoreProto.FactPayload.toDomain(): FactPayload = FactPayload(
    data = data.toByteArray(),
    format = if (hasFormat()) format else null,
    schemaRef = if (hasSchemaRef()) schemaRef else null,
)

internal fun FactStoreProto.StoreInfo.toDomain(): StoreInfo = StoreInfo(
    id = id,
    name = name,
    createdAt = createdAt.toInstant(),
)

internal fun FactStoreProto.ServerInfo.toDomain(): ServerInfo = ServerInfo(
    app = app,
    version = version,
    storageBackend = storageBackend,
)

internal fun com.google.protobuf.Timestamp.toInstant(): Instant =
    Instant.ofEpochSecond(seconds, nanos.toLong())

// ─── Domain → Proto ───────────────────────────────────────────────────────────

internal fun Instant.toProtoTimestamp(): com.google.protobuf.Timestamp = timestamp {
    seconds = epochSecond
    nanos = nano
}

internal fun FactInput.toProto(): FactStoreProto.FactInput = factInput {
    this@toProto.id?.let { id = it }
    type = this@toProto.type
    subject = this@toProto.subject
    this@toProto.appendedAt?.let { appendedAt = it.toProtoTimestamp() }
    payload = this@toProto.payload.toProto()
    metadata.putAll(this@toProto.metadata)
    tags.putAll(this@toProto.tags)
}

internal fun FactPayload.toProto(): FactStoreProto.FactPayload = factPayload {
    data = ByteString.copyFrom(this@toProto.data)
    this@toProto.format?.let { format = it }
    this@toProto.schemaRef?.let { schemaRef = it }
}

internal fun AppendCondition.toProto(): FactStoreProto.AppendCondition = appendCondition {
    when (val c = this@toProto) {
        is AppendCondition.ExpectedLastFact -> expectedLastFact = expectedLastFact {
            subject = c.subject
            c.expectedLastFactId?.let { expectedLastFactId = it }
        }
        is AppendCondition.TagQueryBased -> tagQueryBased = tagQueryBasedCondition {
            failIfEventsMatch = c.failIfEventsMatch.toProto()
            c.afterFactId?.let { afterFactId = it }
        }
        is AppendCondition.All -> all = all {
            conditions += c.conditions.map { it.toProto() }
        }
    }
}

internal fun TagQuery.toProto(): FactStoreProto.TagQuery = tagQuery {
    items += this@toProto.items.map { it.toProto() }
}

internal fun TagQueryItem.toProto(): FactStoreProto.TagQueryItem = tagQueryItem {
    when (val item = this@toProto) {
        is TagQueryItem.TagOnly -> tagOnly = tagOnlyItem { tags.putAll(item.tags) }
        is TagQueryItem.TagType -> tagType = tagTypeItem {
            types += item.types
            tags.putAll(item.tags)
        }
    }
}

internal fun ReadDirection.toProto(): FactStoreProto.ReadDirection = when (this) {
    ReadDirection.FORWARD -> FactStoreProto.ReadDirection.FORWARD
    ReadDirection.BACKWARD -> FactStoreProto.ReadDirection.BACKWARD
}
