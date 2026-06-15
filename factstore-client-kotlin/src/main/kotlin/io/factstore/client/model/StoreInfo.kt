package io.factstore.client.model

import java.time.Instant

data class StoreInfo(
    val id: String,
    val name: String,
    val createdAt: Instant,
)
