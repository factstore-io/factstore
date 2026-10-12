package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.printSingle
import io.factstore.cli.config.FactStoreConfigResolver
import io.factstore.client.FactStoreClient
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.util.UUID
import java.util.concurrent.Callable

@Command(
    name = "get",
    description = ["Get a single fact by its ID"]
)
class GetFactCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Inject
    lateinit var configResolver: FactStoreConfigResolver

    @Parameters(
        index = "0",
        arity = "1",
        description = ["The UUID of the fact to get"],
        paramLabel = "<factId>",
    )
    lateinit var factId: UUID

    @Option(
        names = ["--store", "-s"],
        description = ["The name of the store to query (env: FACTSTORE_STORE, config: store)"],
    )
    var storeName: String? = null

    @Option(
        names = ["--output", "-o"],
        description = ["Output format: \${COMPLETION-CANDIDATES} (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    override fun call(): Int = runBlocking {
        val storeName = configResolver.resolveStore(storeName)
        val fact = client.facts.get(
            storeName = storeName,
            factId = factId.toString()
        )
        fact.printSingle(outputFormat)
        CommandLine.ExitCode.OK
    }
}
