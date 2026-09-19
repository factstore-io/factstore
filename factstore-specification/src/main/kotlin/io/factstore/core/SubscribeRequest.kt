package io.factstore.core

/**
 * Requests a live subscription over the facts of a store.
 *
 * A subscription emits every fact of the store: it has no filters, no limit and no direction, and
 * follows the store's order for as long as it is collected. Reading a part of a store is what the
 * [FactStreamer] operations are for.
 *
 * @property storeName the store to subscribe to
 * @property startPosition where to start reading
 */
data class SubscribeRequest(
    val storeName: StoreName,
    val startPosition: StartPosition = StartPosition.Beginning,
)
