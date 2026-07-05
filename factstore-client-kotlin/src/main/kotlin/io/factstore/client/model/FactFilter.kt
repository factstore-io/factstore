package io.factstore.client.model

import java.time.Instant

/**
 * Composable filter for [FactStoreClient.streamFacts].
 *
 * Mirrors [io.factstore.core.FactFilter] but uses plain Kotlin/Java types so
 * the client library carries no dependency on the specification module.
 */
sealed interface FactFilter {

    /** Match all facts in the store. */
    data object All : FactFilter

    /** Match facts whose subject equals [value]. */
    data class Subject(val value: String) : FactFilter

    /** Match facts whose subject starts with [prefix]. */
    data class SubjectPrefix(val prefix: String) : FactFilter

    /** Match facts whose type equals [value]. */
    data class Type(val value: String) : FactFilter

    /** Match facts that have tag [key]=[value]. */
    data class Tag(val key: String, val value: String) : FactFilter

    /** Match facts that have metadata entry [key]=[value]. */
    data class Metadata(val key: String, val value: String) : FactFilter

    /**
     * Match facts whose `appendedAt` falls in `[from, to)`.
     * A null bound means unbounded in that direction.
     */
    data class TimeRange(val from: Instant? = null, val to: Instant? = null) : FactFilter

    /** Match facts that satisfy at least one of [filters] (OR semantics). */
    data class AnyOf(val filters: List<FactFilter>) : FactFilter

    /** Match facts that satisfy all of [filters] (AND semantics). */
    data class AllOf(val filters: List<FactFilter>) : FactFilter

    /** From all matching facts, select only the first [n] in appended order. */
    data class First(val n: Int, val filter: FactFilter) : FactFilter

    /** From all matching facts, select only the last [n] in appended order. */
    data class Last(val n: Int, val filter: FactFilter) : FactFilter
}
