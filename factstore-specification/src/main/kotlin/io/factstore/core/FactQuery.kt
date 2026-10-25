package io.factstore.core

/**
 * Describes which facts to read from a store.
 *
 * A query is made of [items], and a fact matches the query when it matches **any** of them. Each
 * item describes one kind of fact the reader is interested in, so several items read them
 * together in one stream.
 *
 * A fact matching several items is still one fact: it is streamed once.
 *
 * @property items the items to match against; at least one and at most [MAX_ITEMS]
 *
 * @throws IllegalArgumentException if [items] is empty or holds more than [MAX_ITEMS] items
 *
 * @author Domenic Cassisi
 */
data class FactQuery(val items: List<FactQueryItem>) {

    companion object {

        /**
         * The maximum number of items in one query.
         *
         * A query expresses the interest of one decision, which covers a handful of entities: an
         * order, its customer, the products it reserves. Reading wider than that is a projection
         * or an export, and those stream the whole store rather than enumerate what they want.
         */
        const val MAX_ITEMS = 10
    }

    init {
        require(items.isNotEmpty()) { "A query must contain at least one item." }
        require(items.size <= MAX_ITEMS) {
            "A query must not contain more than $MAX_ITEMS items, but contained ${items.size}."
        }
    }

    /** Whether [fact] matches any of this query's [items]. */
    fun matches(fact: Fact): Boolean = items.any { it.matches(fact) }
}

/**
 * Matches facts by subject, type and tags.
 *
 * Each predicate is optional, and the ones that are set must **all** hold: a fact matches when its
 * subject is one of [subjects], its type is one of [types], and it carries every tag of [tags].
 * An unset predicate does not constrain anything.
 *
 * At least one predicate must be set. An item matching every fact is not expressible on purpose:
 * streaming a whole store is [FactStreamer.streamFacts] without a query.
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
data class FactQueryItem(
    val subjects: Set<Subject> = emptySet(),
    val types: Set<FactType> = emptySet(),
    val tags: Map<TagKey, TagValue> = emptyMap(),
) {

    companion object {

        /**
         * The maximum number of subjects one item matches.
         */
        const val MAX_SUBJECTS = 10

        /**
         * The maximum number of types one item matches.
         */
        const val MAX_TYPES = 10
    }

    init {
        require(subjects.isNotEmpty() || types.isNotEmpty() || tags.isNotEmpty()) {
            "A query item must constrain at least one of subjects, types or tags."
        }
        require(subjects.size <= MAX_SUBJECTS) {
            "A query item must not match more than $MAX_SUBJECTS subjects, but matched ${subjects.size}."
        }
        require(types.size <= MAX_TYPES) {
            "A query item must not match more than $MAX_TYPES types, but matched ${types.size}."
        }
        require(tags.size <= FactInput.MAX_TAGS) {
            "A query item must not require more than ${FactInput.MAX_TAGS} tags, since no fact carries more, but required ${tags.size}."
        }
    }

    /** Whether [fact] satisfies every predicate this item sets. */
    fun matches(fact: Fact): Boolean {
        if (subjects.isNotEmpty() && fact.subject !in subjects) return false
        if (types.isNotEmpty() && fact.type !in types) return false
        return tags.all { (key, value) -> fact.tags[key] == value }
    }
}
