package io.factstore.foundationdb

import io.factstore.core.Fact

data class FdbFact(
    val fact: Fact,
    val factPosition: FactPosition
)
