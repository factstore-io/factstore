package io.factstore.foundationdb

import io.factstore.core.Limit
import io.factstore.core.ReadDirection

/**
 * Converts a [Limit] to the integer expected by FDB's getRange.
 * FDB uses 0 to mean "no limit".
 */
fun Limit.toFdbLimit(): Int = value ?: 0

/**
 * Converts a [ReadDirection] to the reverse boolean expected by FDB's getRange.
 */
fun ReadDirection.isReverse(): Boolean = this == ReadDirection.Backward

/**
 * Comparator for [FactPosition] respecting [ReadDirection].
 * Used when direction must be applied in application code (e.g. after intersection).
 */
fun ReadDirection.toComparator(): Comparator<FactPosition> = when (this) {
    ReadDirection.Forward -> compareBy { it }
    ReadDirection.Backward -> compareByDescending { it }
}

/**
 * Comparator for [FdbFact] respecting [ReadDirection].
 */
fun ReadDirection.toFdbFactComparator(): Comparator<FdbFact> = when (this) {
    ReadDirection.Forward -> compareBy { it.factPosition }
    ReadDirection.Backward -> compareByDescending { it.factPosition }
}
