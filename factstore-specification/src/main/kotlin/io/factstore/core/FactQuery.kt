package io.factstore.core

/**
 * A unified, composable query for selecting facts from a store.
 *
 * A query either matches every fact ([All]) or matches facts against a set of
 * filters ([AnyOf]), allowing more flexible, composed queries.
 *
 * @author Domenic Cassisi
 */
sealed interface FactQuery {

    /**
     * Matches every fact in the store.
     *
     * Combine with a limit and read direction to express operations such as
     * "the last N facts across the whole store".
     */
    data object All : FactQuery

    /**
     * Matches a fact if it satisfies **at least one** of the [filters] (logical OR).
     *
     * @property filters the filters to evaluate; a fact matches when any one of them matches
     *
     * @throws IllegalArgumentException if [filters] is empty
     *
     */
    data class AnyOf(val filters: List<FactFilter>) : FactQuery {
        init {
            require(filters.isNotEmpty()) { "At least one filter must be present." }
        }
    }
}

/**
 * A conjunction of predicates evaluated against a single fact.
 *
 * A fact matches a filter only if **all** of its set predicates hold (logical AND).
 * Each predicate is optional; an unset predicate (an empty collection or `null`)
 * imposes no constraint. At least one predicate must be set — to match every fact,
 * use [FactQuery.All] rather than an empty filter.
 *
 * @property subjects if non-empty, the fact's subject must be one of these (logical OR)
 * @property types if non-empty, the fact's type must be one of these (logical OR)
 * @property tags if non-empty, the fact must carry **all** of these tag key-value pairs
 * @property appendedWithin if non-null, the fact's ingestion time must fall within this range
 *
 * @throws IllegalArgumentException if no predicate is set
 *
 * @author Domenic Cassisi
 */
data class FactFilter(
    val subjects: Set<Subject> = emptySet(),
    val types: Set<FactType> = emptySet(),
    val tags: Map<TagKey, TagValue> = emptyMap(),
    val appendedWithin: TimeRange? = null,
) {
    init {
        require(subjects.isNotEmpty() || types.isNotEmpty() || tags.isNotEmpty() || appendedWithin != null) {
            "A filter must constrain at least one predicate; use FactQuery.All to match every fact."
        }
    }

    fun matches(fact: Fact): Boolean {
        if (subjects.isNotEmpty() && fact.subject !in subjects) return false
        if (types.isNotEmpty() && fact.type !in types) return false
        if (tags.isNotEmpty() && tags.any { (key, value) -> fact.tags[key] != value }) return false
        if (appendedWithin != null && fact.appendedAt !in appendedWithin) return false
        return true
    }
}
