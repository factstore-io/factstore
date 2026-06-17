package io.factstore.core

import java.time.Instant

/**
 * Represents a time interval defined by a [start] and [end] instant.
 *
 * The range is half-open: `[start, end)`. A fact matches if its timestamp is
 * greater than or equal to [start] and strictly less than [end]. A fact whose
 * timestamp equals [end] is therefore **not** included. All FactStore
 * implementations must honor these boundary semantics identically.
 *
 * The range is strictly bounded, meaning:
 * - [start] must be strictly before [end]
 * - equal instants are not allowed
 *
 * @property start the beginning of the time range (inclusive)
 * @property end the end of the time range (exclusive)
 *
 * @throws IllegalArgumentException if [start] is not strictly before [end]
 *
 * Example:
 * ```
 * val range = TimeRange(
 *     start = Instant.parse("2024-01-01T00:00:00Z"),
 *     end = Instant.parse("2024-01-02T00:00:00Z")
 * )
 * ```
 */
data class TimeRange(
    val start: Instant,
    val end: Instant,
) {

    init {
        require(start.isBefore(end)) {
            "start ($start) must be strictly before end ($end)"
        }
    }

    override fun toString(): String = "[$start, $end)"
}
