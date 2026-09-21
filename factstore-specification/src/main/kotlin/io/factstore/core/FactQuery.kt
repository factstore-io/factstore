package io.factstore.core

/**
 * Describes which facts to read from a store.
 *
 * A query is made of [filters], and a fact matches the query when it matches **any** of them. Each
 * filter describes one kind of fact the reader is interested in, so several filters read them
 * together in one stream.
 *
 * A fact matching several filters is still one fact: it is streamed once.
 *
 * @property filters the filters to match against; at least one and at most [MAX_FILTERS]
 *
 * @throws IllegalArgumentException if [filters] is empty or holds more than [MAX_FILTERS] filters
 *
 * @author Domenic Cassisi
 */
data class FactQuery(val filters: List<FactFilter>) {

    companion object {

        /**
         * The maximum number of filters in one query.
         */
        const val MAX_FILTERS = 10
    }

    init {
        require(filters.isNotEmpty()) { "A query must contain at least one filter." }
        require(filters.size <= MAX_FILTERS) {
            "A query must not contain more than $MAX_FILTERS filters, but contained ${filters.size}."
        }
    }
}

/**
 * Matches facts by subject, type and tags.
 *
 * Each predicate is optional, and the ones that are set must **all** hold: a fact matches when its
 * subject is one of [subjects], its type is one of [types], and it carries every tag of [tags].
 * An unset predicate does not constrain anything.
 *
 * At least one predicate must be set. A filter matching every fact is not expressible on purpose:
 * streaming a whole store is [FactStreamer.streamFacts].
 *
 * @property subjects the subjects to match, any of them; at most [MAX_SUBJECTS]
 * @property types the types to match, any of them, each matched exactly; at most [MAX_TYPES]
 * @property tags the tags a fact must carry, all of them; at most [FactInput.MAX_TAGS]
 *
 * @throws IllegalArgumentException if no predicate is set, or a predicate holds more values than
 *         its maximum
 *
 * @author Domenic Cassisi
 */
data class FactFilter(
    val subjects: Set<Subject> = emptySet(),
    val types: Set<FactType> = emptySet(),
    val tags: Map<TagKey, TagValue> = emptyMap(),
) {

    companion object {

        /**
         * The maximum number of subjects one filter matches.
         */
        const val MAX_SUBJECTS = 10

        /**
         * The maximum number of types one filter matches.
         */
        const val MAX_TYPES = 10
    }

    init {
        require(subjects.isNotEmpty() || types.isNotEmpty() || tags.isNotEmpty()) {
            "A filter must constrain at least one of subjects, types or tags."
        }
        require(subjects.size <= MAX_SUBJECTS) {
            "A filter must not match more than $MAX_SUBJECTS subjects, but matched ${subjects.size}."
        }
        require(types.size <= MAX_TYPES) {
            "A filter must not match more than $MAX_TYPES types, but matched ${types.size}."
        }
        requireSatisfiableTagCount(tags)
    }

    /** Whether [fact] satisfies every predicate this filter sets. */
    fun matches(fact: Fact): Boolean {
        if (subjects.isNotEmpty() && fact.subject !in subjects) return false
        if (types.isNotEmpty() && fact.type !in types) return false
        return tags.all { (key, value) -> fact.tags[key] == value }
    }
}
