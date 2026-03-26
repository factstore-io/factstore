package io.factstore.server

import jakarta.enterprise.context.ApplicationScoped
import io.factstore.core.FactStoreFactory
import io.factstore.foundationdb.FdbFactStoreFactory
import io.factstore.foundationdb.FoundationDBFactStoreContext
import jakarta.ws.rs.Produces
import kotlinx.coroutines.runBlocking

@ApplicationScoped
class FactStoreFactoryProvider(
    private val foundationDbContext: FoundationDBFactStoreContext
) {

    @Produces
    @ApplicationScoped
    fun getFactory(): FactStoreFactory = runBlocking {
        FdbFactStoreFactory.create(foundationDbContext)
    }
}
