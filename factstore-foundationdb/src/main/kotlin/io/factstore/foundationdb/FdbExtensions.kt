package io.factstore.foundationdb

import io.factstore.core.ReadDirection

/**
 * Converts a [ReadDirection] to the reverse boolean expected by FDB's getRange.
 */
internal fun ReadDirection.isReverse(): Boolean = this == ReadDirection.Backward

/**
 * Comparator for [FactPosition] respecting [ReadDirection].
 * Used when direction must be applied in application code (e.g. after intersection).
 */
internal fun ReadDirection.toComparator(): Comparator<FactPosition> = when (this) {
    ReadDirection.Forward -> compareBy { it }
    ReadDirection.Backward -> compareByDescending { it }
}
