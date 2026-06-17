package io.factstore.core

import java.time.Instant

/**
 * Represents a half-open time interval `[start, end)` used to query facts by
 * ingestion time.
 *
 * Both bounds are optional. A `null` [start] means unbounded below (from the
 * beginning of the store); a `null` [end] means unbounded above (no upper
 * limit). A fact matches when `start == null || appendedAt >= start` **and**
 * `end == null || appendedAt < end` — so a fact whose timestamp equals [end] is
 * never included.
 *
 * Prefer the [between], [from], [until], and [unbounded] factories for
 * intent-revealing call sites.
 *
 * @property start the inclusive lower bound, or `null` for unbounded below
 * @property end the exclusive upper bound, or `null` for unbounded above
 *
 * @throws IllegalArgumentException if both bounds are present and [start] is not
 *         strictly before [end]
 */
data class TimeRange(
    val start: Instant?,
    val end: Instant?,
) {

    init {
        if (start != null && end != null) {
            require(start.isBefore(end)) {
                "start ($start) must be strictly before end ($end)"
            }
        }
    }

    override fun toString(): String = "[${start ?: "-∞"}, ${end ?: "∞"})"

    companion object {

        /** A bounded range `[start, end)`. */
        fun between(start: Instant, end: Instant) = TimeRange(start, end)

        /** Everything from [start] onwards: `[start, ∞)`. */
        fun from(start: Instant) = TimeRange(start, null)

        /** Everything up to, but excluding, [end]: `[-∞, end)`. */
        fun until(end: Instant) = TimeRange(null, end)

        /** No time bound at all — matches every fact. */
        val unbounded = TimeRange(null, null)
    }
}
