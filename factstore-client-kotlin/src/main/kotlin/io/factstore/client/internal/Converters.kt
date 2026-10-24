package io.factstore.client.internal

import com.google.protobuf.ByteString
import io.factstore.client.model.AppendCondition
import io.factstore.client.model.Fact
import io.factstore.client.model.FactInput
import io.factstore.client.model.FactQuery
import io.factstore.client.model.FactQueryItem
import io.factstore.client.model.FactPayload
import io.factstore.client.model.ReadDirection
import io.factstore.client.model.ServerInfo
import io.factstore.client.model.StoreInfo
import io.factstore.grpc.v1.FactStoreProto
import io.factstore.grpc.v1.all
import io.factstore.grpc.v1.appendCondition
import io.factstore.grpc.v1.expectedLastFact
import io.factstore.grpc.v1.factInput
import io.factstore.grpc.v1.factPayload
import io.factstore.grpc.v1.factQuery
import io.factstore.grpc.v1.factQueryItem
import io.factstore.grpc.v1.noneMatch
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

internal fun FactInput.toProto(): FactStoreProto.FactInput = factInput {
    type = this@toProto.type
    subject = this@toProto.subject
    payload = this@toProto.payload.toProto()
    metadata.putAll(this@toProto.metadata)
    tags.putAll(this@toProto.tags)
}

internal fun FactPayload.toProto(): FactStoreProto.FactPayload = factPayload {
    data = ByteString.copyFrom(this@toProto.data)
}

internal fun AppendCondition.toProto(): FactStoreProto.AppendCondition = appendCondition {
    when (val c = this@toProto) {
        is AppendCondition.ExpectedLastFact -> expectedLastFact = expectedLastFact {
            subject = c.subject
            c.expectedLastFactId?.let { expectedLastFactId = it }
        }
        is AppendCondition.NoneMatch -> noneMatch = noneMatch {
            query = c.query.toProto()
            c.afterFactId?.let { afterFactId = it }
        }
        is AppendCondition.All -> all = all {
            conditions += c.conditions.map { it.toProto() }
        }
    }
}

internal fun FactQuery.toProto(): FactStoreProto.FactQuery = factQuery {
    items += this@toProto.items.map { it.toProto() }
}

internal fun FactQueryItem.toProto(): FactStoreProto.FactQueryItem = factQueryItem {
    subjects += this@toProto.subjects
    types += this@toProto.types
    tags.putAll(this@toProto.tags)
}

internal fun ReadDirection.toProto(): FactStoreProto.ReadDirection = when (this) {
    ReadDirection.FORWARD -> FactStoreProto.ReadDirection.READ_DIRECTION_FORWARD
    ReadDirection.BACKWARD -> FactStoreProto.ReadDirection.READ_DIRECTION_BACKWARD
}
