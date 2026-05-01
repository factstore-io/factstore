package io.factstore.core

import java.time.Instant

data class StoreMetadata(
    val id: StoreId,
    val name: StoreName,
    val createdAt: Instant,
)
