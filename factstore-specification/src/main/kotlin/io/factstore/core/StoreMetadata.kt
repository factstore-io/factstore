package io.factstore.core

import java.time.Instant

/**
 * What a FactStore knows about one of its stores.
 *
 * @property id the identity of the store, assigned when it was created
 * @property name the name the store is addressed by
 * @property createdAt when the store was created, by the server's clock
 */
data class StoreMetadata(
    val id: StoreId,
    val name: StoreName,
    val createdAt: Instant,
)
