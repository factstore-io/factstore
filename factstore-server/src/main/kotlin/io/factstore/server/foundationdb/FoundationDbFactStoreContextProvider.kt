package io.factstore.server.foundationdb

import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces
import io.factstore.foundationdb.FoundationDBFactStoreContext
import io.factstore.foundationdb.initDatabase
import io.factstore.server.config.FactStoreConfig

@ApplicationScoped
class FoundationDbFactStoreContextProvider(
    val config: FactStoreConfig
) {

    @Produces
    @ApplicationScoped
    fun fdbDatabaseContext(): FoundationDBFactStoreContext {
        return initDatabase(
            clusterFilePath = config.foundationdb().clusterFile(),
            apiVersion = config.foundationdb().apiVersion()
        )
    }

}
