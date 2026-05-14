package io.factstore.server.info

import io.factstore.server.config.FactStoreConfig
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class ServerInfoProducer {

    @Produces
    @ApplicationScoped
    fun produceServerInfo(
        config: FactStoreConfig,
        @ConfigProperty(name = "quarkus.application.name", defaultValue = "factstore-server")
        appName: String,
        @ConfigProperty(name = "quarkus.application.version")
        version: String,
    ) = ServerInfo(
        app = appName,
        version = version,
        storageBackend = config.storage().type()
    )

}