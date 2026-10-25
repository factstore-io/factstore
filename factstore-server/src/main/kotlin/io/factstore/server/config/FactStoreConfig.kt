package io.factstore.server.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.ConfigMapping.NamingStrategy.KEBAB_CASE

@ConfigMapping(
    prefix = "factstore",
    namingStrategy = KEBAB_CASE,
)
interface FactStoreConfig {

    fun storage(): StorageConfig

    fun foundationdb(): FoundationDbConfig

    interface StorageConfig {
        fun type(): StorageType
    }

    interface FoundationDbConfig {
        fun clusterFilePath(): String
        fun apiVersion(): Int
    }
}

/** The backends the server can store facts in, as `factstore.storage.type` names them. */
enum class StorageType { MEMORY, FOUNDATIONDB }
