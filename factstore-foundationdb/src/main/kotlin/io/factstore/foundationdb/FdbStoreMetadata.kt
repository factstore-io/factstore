package io.factstore.foundationdb

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.util.UUID

@Serializable
data class FdbStoreMetadata(
    @Contextual
    val storeId: UUID,
    val name: String,
    val createdAtEpochSeconds: Long
)
