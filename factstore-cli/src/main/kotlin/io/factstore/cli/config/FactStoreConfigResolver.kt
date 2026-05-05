package io.factstore.cli.config

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
            ?: System.getenv("FACTSTORE_URL")
            ?: configService.getUrl()
            ?: "http://localhost:8080/api"

        return URI.create(resolved)
    }

}
