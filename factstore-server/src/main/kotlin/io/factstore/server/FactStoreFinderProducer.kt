package io.factstore.server

import io.factstore.core.FactStoreFinder
import io.factstore.foundationdb.FdbFactStoreFinder
import io.factstore.foundationdb.FoundationDBFactStoreContext
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces

@ApplicationScoped
class FactStoreFinderProducer {

    @Produces
    @ApplicationScoped
    fun factStoreFinder(context: FoundationDBFactStoreContext): FactStoreFinder {
        return FdbFactStoreFinder(context)
    }

}
