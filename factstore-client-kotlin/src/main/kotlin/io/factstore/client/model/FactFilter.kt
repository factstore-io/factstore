package io.factstore.client.model

/**
 * Matches facts by subject, type and tags.
 *
 * Every property that is set must hold, while a property with several values matches any of them.
 * At least one property must be set.
 */
data class FactFilter(
    val subjects: List<String> = emptyList(),
    val types: List<String> = emptyList(),
    val tags: Map<String, String> = emptyMap(),
)
