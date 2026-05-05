package io.factstore.cli.client

import io.factstore.cli.config.FactStoreConfigResolver
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import picocli.CommandLine

@ApplicationScoped
class FactStoreClientProducer {

    @Produces
    @ApplicationScoped
    fun produceClient(
        parseResult: CommandLine.ParseResult,
        resolver: FactStoreConfigResolver,
        factory: FactStoreClientFactory
    ): FactStoreClient {
        val url = resolver.resolveUrl(parseResult)
        return factory.create(url)
    }
}
