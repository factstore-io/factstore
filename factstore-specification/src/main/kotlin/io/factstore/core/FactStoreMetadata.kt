package io.factstore.core

import java.time.Instant

data class FactStoreMetadata(
    val id: FactStoreId,
    val name: FactStoreName,
    val createdAt: Instant,
)
