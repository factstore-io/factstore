package io.factstore.server.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.ConfigMapping.NamingStrategy.KEBAB_CASE

@ConfigMapping(
    prefix = "factstore",
    namingStrategy = KEBAB_CASE,
)
interface FactStoreConfig {

    fun foundationdb(): FoundationDbConfig

    interface FoundationDbConfig {
        fun clusterFile(): String
        fun apiVersion(): Int
    }
}
