package io.factstore.client.model

/**
 * Selects facts: a fact matches the query when it matches any of its [items], and a fact matching
 * several of them is still one fact.
 */
data class FactQuery(val items: List<FactQueryItem>) {
    constructor(vararg items: FactQueryItem) : this(items.toList())
}

/**
 * Matches facts by subject, type and tags.
 *
 * Every property that is set must hold, while a property with several values matches any of them.
 * At least one property must be set.
 */
data class FactQueryItem(
    val subjects: List<String> = emptyList(),
    val types: List<String> = emptyList(),
    val tags: Map<String, String> = emptyMap(),
)
