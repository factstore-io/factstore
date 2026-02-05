package org.factstore.server

import jakarta.enterprise.context.ApplicationScoped
import org.factstore.core.FactStore
import org.factstore.foundationdb.FoundationDBFactStoreContext
import org.factstore.foundationdb.buildFdbFactStore

@ApplicationScoped
class FactStoreProvider(
    private val foundationDbContext: FoundationDBFactStoreContext
) {

    // TODO make thread safe
    private val factStores = mutableMapOf<String, FactStore>()

    fun findByName(factStoreName: String): FactStore =
        factStores[factStoreName] ?: createFactStore(factStoreName)

    private fun createFactStore(factStoreName: String): FactStore {
        val factStore = buildFdbFactStore(
            context = foundationDbContext,
            name = factStoreName
        )
        factStores[factStoreName] = factStore
        return factStore
    }

}
