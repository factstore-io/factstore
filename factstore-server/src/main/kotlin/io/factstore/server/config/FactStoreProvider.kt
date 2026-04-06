package io.factstore.server.config

import io.factstore.core.FactStore
import io.factstore.memory.MemoryFactStore
import io.factstore.foundationdb.buildFdbFactStore
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces
import kotlinx.coroutines.runBlocking

/**
 * Dynamic FactStore provider that chooses the implementation based on configuration.
 *
 * This provider reads the `factstore.storage.type` configuration property at runtime
 * and instantiates the appropriate FactStore implementation:
 * - "memory": Uses the in-memory implementation for testing
 * - "foundationdb": Uses the FoundationDB implementation for production
 *
 * The configuration is dynamic and can be changed at runtime without rebuilding.
 * This allows the same application to run with different storage backends
 * depending on the deployment environment.
 */
@ApplicationScoped
class FactStoreProvider(
    val config: FactStoreConfig
) {

    companion object {

        const val STORAGE_TYPE_MEMORY = "memory"
        const val STORAGE_TYPE_FOUNDATIONDB = "foundationdb"

    }

    @Produces
    @ApplicationScoped
    fun factStore(): FactStore = runBlocking {
        when (val storageType = config.storage().type().lowercase()) {
            STORAGE_TYPE_MEMORY -> MemoryFactStore()
            STORAGE_TYPE_FOUNDATIONDB -> buildFdbFactStore()

            else -> {
                throw IllegalArgumentException(
                    "Unsupported storage type: $storageType. " +
                    "Supported types: memory, foundationdb"
                )
            }
        }
    }

    private suspend fun buildFdbFactStore(): FactStore =
        buildFdbFactStore(
            clusterFilePath = config.foundationdb().clusterFilePath(),
            apiVersion = config.foundationdb().apiVersion()
        )
}
