package io.factstore.application

import io.factstore.core.FactId

fun interface FactIdGenerator {

    fun generate(): FactId
}
