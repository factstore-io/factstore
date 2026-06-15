package io.factstore.core

data class StreamFactsRequest(
    val storeName: StoreName,
    val startPosition: StartPosition = StartPosition.Beginning,
)
