package io.factstore.server

import io.factstore.core.FactStoreFactory
import io.factstore.foundationdb.FdbFactStoreFactory
import io.factstore.foundationdb.FoundationDBFactStoreContext
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces

@ApplicationScoped
class FactStoreFactoryProducer {

    @Produces
    @ApplicationScoped
    fun factStoreFactory(
        foundationDBFactStoreContext: FoundationDBFactStoreContext
    ): FactStoreFactory =
        FdbFactStoreFactory(foundationDBFactStoreContext)

}
