package io.factstore.server.info

data class ServerInfo(
    val app: String,
    val version: String,
    val storageBackend: String,
)
