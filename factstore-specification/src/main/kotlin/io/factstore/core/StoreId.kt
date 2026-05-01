package io.factstore.core

import java.util.UUID

@JvmInline
value class StoreId(val uuid: UUID) {

    companion object {

        fun generate() = StoreId(UUID.randomUUID())

    }

}
