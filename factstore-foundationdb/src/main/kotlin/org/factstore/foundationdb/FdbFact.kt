package org.factstore.foundationdb

import com.apple.foundationdb.tuple.Tuple
import org.factstore.core.Fact

data class FdbFact(
    val fact: Fact,
    val positionTuple: Tuple
)
