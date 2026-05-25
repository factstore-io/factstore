package io.factstore.cli.config

import io.factstore.cli.exception.CliUsageException
import jakarta.enterprise.context.ApplicationScoped
import picocli.CommandLine
import java.net.URI

@ApplicationScoped
class FactStoreConfigResolver(
    private val configService: ConfigService,
) {

    fun resolveUrl(parseResult: CommandLine.ParseResult): URI {
        val flagUrl =
            parseResult.asCommandLineList()
                .reversed()
                .firstNotNullOfOrNull { it.parseResult.matchedOption("--url") }
                ?.getValue<String>()

        val resolved = flagUrl
            ?: env("FACTSTORE_URL")
            ?: configService.getUrl()
            ?: "http://localhost:8080/api"

        return URI.create(resolved)
    }

    fun resolveStore(storeName: String?): String =
        storeName
            ?: env("FACTSTORE_STORE")
            ?: configService.getStore()
            ?: throw CliUsageException(
                "No store specified. Use --store / -s or set the FACTSTORE_STORE environment variable."
            )

    private fun env(name: String): String? = System.getenv(name)

}
