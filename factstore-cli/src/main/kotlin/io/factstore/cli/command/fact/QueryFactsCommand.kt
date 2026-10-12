package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.print
import io.factstore.cli.command.requirePositiveLimit
import io.factstore.cli.exception.CliUsageException
import io.factstore.cli.config.FactStoreConfigResolver
import io.factstore.client.FactStoreClient
import io.factstore.client.model.FactFilter
import io.factstore.client.model.ReadDirection
import jakarta.inject.Inject
import io.factstore.client.model.Fact
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.io.File
import java.util.UUID
import java.util.concurrent.Callable

@Command(
    name = "query",
    description = [
        "Stream the facts matching a query: a fact matches when it matches any of its filters.",
        "Each filter is JSON; for a single filter 'fact stream' is the simpler command.",
    ]
)
class QueryFactsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Inject
    lateinit var configResolver: FactStoreConfigResolver

    @Option(
        names = ["--store", "-s"],
        description = ["The name of the store to query (env: FACTSTORE_STORE, config: store)"],
    )
    var storeName: String? = null

    @Option(
        names = ["--filter"],
        description = ["A filter as JSON, repeatable, e.g. '{\"types\":[\"OrderPlaced\"],\"tags\":{\"region\":\"eu\"}}'"],
    )
    var filters: MutableList<String> = mutableListOf()

    @Option(
        names = ["--query-file"],
        description = ["A file holding the query as JSON: {\"filters\":[…]}"],
    )
    var queryFile: File? = null

    @Option(
        names = ["--continue-after"],
        description = ["Continue after this fact, exclusive and in reading order"],
        paramLabel = "<factId>",
    )
    var continueAfter: UUID? = null

    @Option(
        names = ["--limit"],
        description = ["Maximum number of facts to return, a positive number (default: all facts)"],
    )
    var limit: Int? = null

    @Option(
        names = ["--direction"],
        description = ["Read direction: \${COMPLETION-CANDIDATES} (default: \${DEFAULT-VALUE})"],
        defaultValue = "forward",
    )
    lateinit var direction: ReadDirection

    @Option(
        names = ["--output", "-o"],
        description = ["Output format: \${COMPLETION-CANDIDATES} (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    override fun call(): Int = runBlocking {
        requirePositiveLimit(limit)
        val storeName = configResolver.resolveStore(storeName)
        val filters = collectFilters()
        if (filters.isEmpty()) {
            throw CliUsageException("A query needs at least one filter: use --filter or --query-file.")
        }

        val facts: Flow<Fact> = client.facts.streamFactsByQuery(
            storeName = storeName,
            filters = filters,
            direction = direction,
            limit = limit,
            continueAfter = continueAfter?.toString(),
        )

        facts.print(outputFormat)

        CommandLine.ExitCode.OK
    }

    private fun collectFilters(): List<FactFilter> =
        filters.map(::parseFilter) + queryFile?.let(::readQueryFile).orEmpty()
}
