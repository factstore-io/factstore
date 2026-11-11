package io.factstore.application

import io.factstore.core.StoreId

fun interface StoreIdGenerator {

    fun generate(): StoreId
}
