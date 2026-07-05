package io.factstore.client.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.time.Instant

/**
 * Composable filter for [FactStoreClient.streamFacts].
 *
 * Mirrors [io.factstore.core.FactFilter] but uses plain Kotlin/Java types so
 * the client library carries no dependency on the specification module.
 *
 * `@SerialName` values deliberately match the discriminator strings used by the
 * HTTP API's `FactFilterHttp` (Jackson `@JsonTypeInfo`/`@JsonSubTypes`, see
 * `factstore-server`'s `http/api.kt`): the two are independent (de)serializers —
 * kotlinx.serialization here, Jackson there — for the same JSON contract, so a
 * `--filter` JSON body written for one is valid for the other.
 */
@Serializable
sealed interface FactFilter {

    /** Match all facts in the store. */
    @Serializable
    @SerialName("all")
    data object All : FactFilter

    /** Match facts whose subject equals [value]. */
    @Serializable
    @SerialName("subject")
    data class Subject(val value: String) : FactFilter

    /** Match facts whose subject starts with [prefix]. */
    @Serializable
    @SerialName("subjectPrefix")
    data class SubjectPrefix(val prefix: String) : FactFilter

    /** Match facts whose type equals [value]. */
    @Serializable
    @SerialName("type")
    data class Type(val value: String) : FactFilter

    /** Match facts that have tag [key]=[value]. */
    @Serializable
    @SerialName("tag")
    data class Tag(val key: String, val value: String) : FactFilter

    /** Match facts that have metadata entry [key]=[value]. */
    @Serializable
    @SerialName("metadata")
    data class Metadata(val key: String, val value: String) : FactFilter

    /**
     * Match facts whose `appendedAt` falls in `[from, to)`.
     * A null bound means unbounded in that direction.
     */
    @Serializable
    @SerialName("timeRange")
    data class TimeRange(
        @Serializable(with = InstantSerializer::class) val from: Instant? = null,
        @Serializable(with = InstantSerializer::class) val to: Instant? = null,
    ) : FactFilter

    /** Match facts that satisfy at least one of [filters] (OR semantics). */
    @Serializable
    @SerialName("anyOf")
    data class AnyOf(val filters: List<FactFilter>) : FactFilter

    /** Match facts that satisfy all of [filters] (AND semantics). */
    @Serializable
    @SerialName("allOf")
    data class AllOf(val filters: List<FactFilter>) : FactFilter

    /** From all matching facts, select only the first [n] in appended order. */
    @Serializable
    @SerialName("first")
    data class First(val n: Int, val filter: FactFilter) : FactFilter

    /** From all matching facts, select only the last [n] in appended order. */
    @Serializable
    @SerialName("last")
    data class Last(val n: Int, val filter: FactFilter) : FactFilter
}
