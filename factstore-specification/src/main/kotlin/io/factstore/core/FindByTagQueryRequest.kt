package io.factstore.core

data class FindByTagQueryRequest(
    val storeName: StoreName,
    val query: TagQuery,
)
