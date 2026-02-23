package org.factstore.foundationdb

import org.factstore.core.Fact

data class FdbFact(
    val fact: Fact,
    val factPosition: FactPosition
)
