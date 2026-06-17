package io.factstore.core

data class FindByTagsRequest(
    val storeName: StoreName,
    val tags: Map<TagKey, TagValue>,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
)
