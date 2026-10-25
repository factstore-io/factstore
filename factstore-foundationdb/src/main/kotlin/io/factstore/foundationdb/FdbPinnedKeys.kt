package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import io.factstore.core.ReadDirection

/**
 * The keys of [range] up to and including [pinnedEndKey], read in [direction].
 *
 * Without a pinned key, the keys run to the end of the range. That is how an append condition
 * reads within its transaction: a fact appended concurrently lands at the range's end, and only a
 * read that covers it there makes the two transactions conflict.
 *
 * A read starts at one end and continues from its cursor, the last key read, towards the
 * other end: reading forward moves the begin past the cursor, reading backward moves the end
 * before it.
 */
internal class PinnedKeys(range: Range, pinnedEndKey: ByteArray?, direction: ReadDirection) {

    val reverse = direction.isReverse()

    private val first = KeySelector.firstGreaterOrEqual(range.begin)

    // An exclusive end that includes the pinned key itself.
    private val last = pinnedEndKey?.let { KeySelector.firstGreaterThan(it) } ?: KeySelector.firstGreaterOrEqual(range.end)

    /** The begin and end of the keys still to be read after [cursor]; all of them if it is `null`. */
    fun remainingAfter(cursor: ByteArray?): Pair<KeySelector, KeySelector> = when {
        cursor == null -> first to last
        reverse -> first to KeySelector.firstGreaterOrEqual(cursor)
        else -> KeySelector.firstGreaterThan(cursor) to last
    }

    /** The begin and end of the keys from [key] onwards, [key] included. */
    fun remainingFrom(key: ByteArray): Pair<KeySelector, KeySelector> =
        if (reverse) first to KeySelector.firstGreaterThan(key)
        else KeySelector.firstGreaterOrEqual(key) to last

    /** Orders positions the way this scan reads them. */
    val readingOrder: Comparator<FactPosition> = direction.toComparator()
}

/**
 * Converts a [ReadDirection] to the reverse boolean expected by FDB's getRange.
 */
internal fun ReadDirection.isReverse(): Boolean = this == ReadDirection.Backward

/**
 * Orders positions the way a read in this direction visits them: ascending forward, descending
 * backward. Used wherever positions are compared in application code, such as when cursors are
 * merged into a union or an intersection.
 */
internal fun ReadDirection.toComparator(): Comparator<FactPosition> = when (this) {
    ReadDirection.Forward -> compareBy { it }
    ReadDirection.Backward -> compareByDescending { it }
}
