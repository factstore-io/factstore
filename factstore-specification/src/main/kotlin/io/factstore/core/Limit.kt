package io.factstore.core

/**
 * Represents an optional upper bound on the number of facts returned by a read operation.
 *
 * Use [Limit.of] to cap results to a specific count, or [Limit.None] to retrieve
 * all matching facts without restriction.
 *
 * @property value the maximum number of facts to return, or `null` if unbounded.
 */
@JvmInline
value class Limit private constructor(val value: Int?) {

    companion object {

        /**
         * A [Limit] that applies no restriction — all matching facts are returned.
         *
         * This is the default for all read operations. Use with caution on large
         * stores, as it may return an unbounded number of facts.
         */
        val None: Limit = Limit(null)

        /**
         * Creates a [Limit] capped at the given positive [value].
         *
         * @param value the maximum number of facts to return. Must be greater than zero.
         * @throws IllegalArgumentException if [value] is not positive.
         */
        fun of(value: Int): Limit {
            require(value > 0) { "Limit must be positive, got: $value" }
            return Limit(value)
        }
    }
}
