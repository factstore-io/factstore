package io.factstore.client.model

data class ServerInfo(
    val app: String,
    val version: String,
    val storageBackend: String,
)
