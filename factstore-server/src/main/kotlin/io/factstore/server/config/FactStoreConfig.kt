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
        fun type(): String
    }

    interface FoundationDbConfig {
        fun clusterFilePath(): String
        fun apiVersion(): Int
    }
}
