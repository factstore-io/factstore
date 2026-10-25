package io.factstore.foundationdb

import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.StoreId
import io.factstore.core.StoreMetadata
import io.factstore.core.StoreName
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import java.time.Instant
import java.util.UUID

@Serializable
internal data class FdbStoreMetadata(
    @Contextual
    val storeId: UUID,
    val name: String,
    val createdAtEpochSeconds: Long,
    val createdAtNanos: Int,
)

internal fun FdbStoreMetadata.encode(): ByteArray = Avro.encodeToByteArray(this)

internal fun ByteArray.toFdbStoreMetadata(): FdbStoreMetadata = Avro.decodeFromByteArray(this)

internal fun StoreMetadata.toFdbStoreMetadata(): FdbStoreMetadata = FdbStoreMetadata(
    storeId = id.uuid,
    name = name.value,
    createdAtEpochSeconds = createdAt.epochSecond,
    createdAtNanos = createdAt.nano,
)

internal fun FdbStoreMetadata.toStoreMetadata(): StoreMetadata = StoreMetadata(
    id = StoreId(storeId),
    name = StoreName(name),
    createdAt = Instant.ofEpochSecond(createdAtEpochSeconds, createdAtNanos.toLong()),
)
