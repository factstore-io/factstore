package io.factstore.core

import java.util.UUID

@JvmInline
value class FactStoreId(val uuid: UUID) {

    companion object {

        fun generate() = FactStoreId(UUID.randomUUID())

    }

}
