package io.factstore.core

data class FactQueryRequest(
    val storeName: StoreName,
    val query: FactQuery,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
    val after: FactId? = null,
)
