package io.factstore.core

data class ReplayRequest(
    val storeName: StoreName,
    val start: ReplayStart = ReplayStart.Beginning,
)
