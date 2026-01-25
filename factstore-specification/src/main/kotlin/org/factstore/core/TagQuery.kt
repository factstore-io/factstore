package org.factstore.core

/**
 * Represents a single item within a [TagQuery].
 *
 * A tag query item defines a logical constraint that can be evaluated against
 * stored facts. Multiple query items can be combined to express more complex
 * matching conditions.
 *
 * @author Domenic Cassisi
 */
sealed interface TagQueryItem

/**
 * A query item that matches facts by both fact type and tags.
 *
 * A fact matches this query item if its type is contained in [types] and it
 * contains all specified tag key-value pairs.
 *
 * @property types the allowed fact types to match
 * @property tags the required tag key-value pairs
 *
 * @throws IllegalArgumentException if either [types] or [tags] is empty
 *
 * @author Domenic Cassisi
 */
data class TagTypeItem(
    val types: List<FactType>,
    val tags: List<Pair<TagKey, TagValue>>
) : TagQueryItem {
    init {
        require(types.isNotEmpty() && tags.isNotEmpty()) {
            "Both types and tags must be defined!"
        }
    }
}

/**
 * A query item that matches facts based solely on tags.
 *
 * A fact matches this query item if it contains all specified tag key-value
 * pairs, regardless of its type.
 *
 * @property tags the required tag key-value pairs
 *
 * @throws IllegalArgumentException if [tags] is empty
 *
 * @author Domenic Cassisi
 */
data class TagOnlyQueryItem(
    val tags: List<Pair<TagKey, TagValue>>
) : TagQueryItem {
    init {
        require(tags.isNotEmpty()) { "Tags must be defined!" }
    }
}

/**
 * Represents a tag-based query used to select facts from the store.
 *
 * A [TagQuery] consists of one or more [TagQueryItem]s. A fact matches the query
 * if it matches at least one of the contained query items (logical OR).
 *
 * @property queryItems the list of query items that make up this query
 *
 * @throws IllegalArgumentException if [queryItems] is empty
 *
 * @author Domenic Cassisi
 */
data class TagQuery(
    val queryItems: List<TagQueryItem>
) {
    init {
        require(queryItems.isNotEmpty()) {
            "At least one query item must be present!"
        }
    }
}
