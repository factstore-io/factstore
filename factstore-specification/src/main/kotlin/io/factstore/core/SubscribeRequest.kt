package io.factstore.core

data class SubscribeRequest(
    val storeName: StoreName,
    val startPosition: StartPosition = StartPosition.Beginning,
)
