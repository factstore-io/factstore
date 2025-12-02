package org.factstore.core

sealed interface FactQuery

sealed interface TagQueryItem

data class TagTypeItem(
    val types: List<String>,
    val tags: List<Pair<String, String>>
) : TagQueryItem {
    init {
        require((types.isNotEmpty() && tags.isNotEmpty())) { "Both types and tags must be defined!" }
    }
}

data class TagOnlyQueryItem(val tags: List<Pair<String, String>>) : TagQueryItem {
    init {
        require(tags.isNotEmpty()) { "Tags must be defined!" }
    }
}

data class TagQuery(
    val queryItems: List<TagQueryItem>
) : FactQuery {

    init {
        require(queryItems.isNotEmpty()) { "At least one query item must be present!" }
    }
}