package io.factstore.client.model

data class TagQuery(val items: List<TagQueryItem>)

sealed class TagQueryItem {
    data class TagOnly(val tags: Map<String, String>) : TagQueryItem()
    data class TagType(val types: List<String>, val tags: Map<String, String>) : TagQueryItem()
}
