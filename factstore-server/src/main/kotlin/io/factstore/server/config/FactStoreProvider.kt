package io.factstore.server.config

import io.factstore.core.FactStore
import io.factstore.foundationdb.buildFdbFactStore
import io.factstore.memory.MemoryFactStore
import io.github.oshai.kotlinlogging.KotlinLogging
import io.quarkus.runtime.Startup
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces
import kotlinx.coroutines.runBlocking

private val logger = KotlinLogging.logger {}

/**
 * Dynamic FactStore provider that chooses the implementation based on configuration.
 *
 * This provider reads the `factstore.storage.type` configuration property once, at startup,
 * and instantiates the appropriate FactStore implementation:
 * - "memory": Uses the in-memory implementation for testing
 * - "foundationdb": Uses the FoundationDB implementation for production
 *
 * The property is read from the runtime configuration, so the same build can run with either
 * backend; changing it takes a restart.
 */
@ApplicationScoped
class FactStoreProvider(
    val config: FactStoreConfig
) {

    companion object {

        const val STORAGE_TYPE_MEMORY = "memory"
        const val STORAGE_TYPE_FOUNDATIONDB = "foundationdb"

    }

    @Startup
    @Produces
    @ApplicationScoped
    fun factStore(): FactStore = runBlocking {
        val storageType = config.storage().type().lowercase()
        val store = when (storageType) {
            STORAGE_TYPE_MEMORY -> MemoryFactStore()
            STORAGE_TYPE_FOUNDATIONDB -> buildFdbFactStore()

            else -> {
                throw IllegalArgumentException(
                    "Unsupported storage type: $storageType. " +
                            "Supported types: memory, foundationdb"
                )
            }
        }

        logger.info { "FactStore initialized successfully with type: $storageType" }
        store
    }

    private suspend fun buildFdbFactStore(): FactStore =
        buildFdbFactStore(
            clusterFilePath = config.foundationdb().clusterFilePath(),
            apiVersion = config.foundationdb().apiVersion()
        )
}
