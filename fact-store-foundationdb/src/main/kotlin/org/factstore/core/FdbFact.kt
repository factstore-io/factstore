package org.factstore.core

import com.apple.foundationdb.tuple.Tuple

data class FdbFact(
    val fact: Fact,
    val positionTuple: Tuple
)
