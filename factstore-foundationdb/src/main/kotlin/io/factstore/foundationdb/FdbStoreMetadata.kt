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
data class FdbStoreMetadata(
    @Contextual
    val storeId: UUID,
    val name: String,
    val createdAtEpochSeconds: Long,
    val createdAtNanos: Int,
)

fun FdbStoreMetadata.encode(): ByteArray = Avro.encodeToByteArray(this)

fun ByteArray.toFdbStoreMetadata(): FdbStoreMetadata = Avro.decodeFromByteArray(this)

fun FdbStoreMetadata.toStoreMetadata(): StoreMetadata = StoreMetadata(
    id = StoreId(storeId),
    name = StoreName(name),
    createdAt = Instant.ofEpochSecond(createdAtEpochSeconds, createdAtNanos.toLong()),
)
