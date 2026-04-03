package io.factstore.server.foundationdb

import io.factstore.core.FactStore
import io.factstore.core.FactStoreFactory
import io.factstore.core.FactStoreFinder
import io.factstore.foundationdb.FdbFactStoreFactory
import io.factstore.foundationdb.FdbFactStoreFinder
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces
import io.factstore.foundationdb.FoundationDBFactStoreContext
import io.factstore.foundationdb.buildFdbFactStore
import io.factstore.foundationdb.initDatabase
import io.factstore.server.config.FactStoreConfig
import kotlinx.coroutines.runBlocking

@ApplicationScoped
class FoundationDbFactStoreContextProvider(
    val config: FactStoreConfig
) {

    @Produces
    @ApplicationScoped
    fun foundationDBFactStoreContext(): FoundationDBFactStoreContext {
        return initDatabase(
            clusterFilePath = config.foundationdb().clusterFile(),
            apiVersion = config.foundationdb().apiVersion()
        )
    }

    @Produces
    @ApplicationScoped
    fun factStore(): FactStore = runBlocking {
        buildFdbFactStore()
    }

    @Produces
    @ApplicationScoped
    fun factStoreFactory(
        foundationDBFactStoreContext: FoundationDBFactStoreContext
    ): FactStoreFactory =
        FdbFactStoreFactory(foundationDBFactStoreContext)

    @Produces
    @ApplicationScoped
    fun factStoreFinder(context: FoundationDBFactStoreContext): FactStoreFinder {
        return FdbFactStoreFinder(context)
    }

}
