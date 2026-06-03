package io.factstore.core

data class FindByTagsRequest(
    val storeName: StoreName,
    val tags: List<Pair<TagKey, TagValue>>,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
)
