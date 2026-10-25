package io.factstore.core

import java.util.UUID

/**
 * The identity of a store, assigned when it is created.
 *
 * Unlike its [StoreName], the id of a store never changes and is never reused.
 *
 * @property uuid the underlying UUID value
 */
@JvmInline
value class StoreId(val uuid: UUID) {

    companion object {

        /** Generates a new random [StoreId]. */
        fun generate() = StoreId(UUID.randomUUID())

    }

}
