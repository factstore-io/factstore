package org.factstore.server.foundationdb

import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces
import org.factstore.foundationdb.FoundationDBFactStoreContext
import org.factstore.foundationdb.initDatabase
import org.factstore.server.config.FactStoreConfig

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
