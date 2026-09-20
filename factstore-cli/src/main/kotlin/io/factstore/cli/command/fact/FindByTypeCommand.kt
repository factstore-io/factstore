package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.print
import io.factstore.client.FactStoreClient
import io.factstore.client.model.ReadDirection
import jakarta.inject.Inject
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.util.concurrent.Callable

@Command(
    name = "find-by-type",
    description = ["Find facts by their type, matched exactly"]
)
class FindByTypeCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Parameters(
        index = "0",
        arity = "1",
        description = ["The type for which to find facts"],
        paramLabel = "<type>"
    )
    lateinit var type: String

    @Option(
        names = ["--store", "-s"],
        required = true,
        description = ["The name of the store to query (env: FACTSTORE_STORE, config: store)"],
    )
    lateinit var storeName: String

    @Option(
        names = ["--limit"],
        required = false,
        description = ["Maximum number of facts to return (default: \${DEFAULT-VALUE})"],
        defaultValue = "100",
    )
    var limit: Int = 100

    @Option(
        names = ["--direction"],
        description = ["Read direction: \${COMPLETION-CANDIDATES} (default: \${DEFAULT-VALUE})"],
        defaultValue = "forward",
    )
    lateinit var direction: ReadDirection

    @Option(
        names = ["--output", "-o"],
        description = ["Output format (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    override fun call(): Int = runBlocking {
        // Collected in full: the table needs every row to size its columns, and --limit bounds it.
        val facts = client.facts.streamFactsByType(
            storeName = storeName,
            type = type,
            direction = direction,
            limit = limit,
        ).toList()

        facts.print(outputFormat)

        CommandLine.ExitCode.OK
    }

}
