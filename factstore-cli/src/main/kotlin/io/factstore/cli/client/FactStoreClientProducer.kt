package io.factstore.cli.client

import io.factstore.client.FactStoreClient
import io.grpc.Channel
import io.quarkus.grpc.GrpcClient
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.Produces

@ApplicationScoped
class FactStoreClientProducer {

    @Produces
    @ApplicationScoped
    fun factStoreClient(
        @GrpcClient("factstore") channel: Channel
    ): FactStoreClient =
        io.factstore.client.factStoreClient(channel)

}