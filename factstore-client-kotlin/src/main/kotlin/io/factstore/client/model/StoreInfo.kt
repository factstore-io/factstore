package io.factstore.client.model

import kotlinx.serialization.Serializable
import java.time.Instant

@Serializable
data class StoreInfo(
    val id: String,
    val name: String,
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant,
)
