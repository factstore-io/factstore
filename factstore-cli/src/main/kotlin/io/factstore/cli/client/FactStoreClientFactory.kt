package io.factstore.cli.client

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder
import jakarta.enterprise.context.ApplicationScoped
import java.net.URI

@ApplicationScoped
class FactStoreClientFactory {

    fun create(url: URI): FactStoreClient {
        return QuarkusRestClientBuilder
            .newBuilder()
            .baseUri(url)
            .build(FactStoreClient::class.java)
    }

}
