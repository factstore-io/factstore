package io.factstore.cli.command

import io.factstore.cli.client.FactStoreClient
import jakarta.inject.Inject
import picocli.CommandLine.Command
import picocli.CommandLine.ExitCode
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.util.UUID
import java.util.concurrent.Callable

@Command(
    name = "fact",
    description = ["Find a single fact by its ID"]
)
class FindFactCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Parameters(
        index = "0",
        arity = "1",
        description = ["The ID of the fact to retrieve"],
        paramLabel = "<factId>"
    )
    lateinit var factId: UUID

    @Option(
        names = ["--store", "-s"],
        description = ["The name of the store to query"],
        required = true,
    )
    lateinit var storeName: String

    @Option(
        names = ["--output", "-o"],
        description = ["Output format (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    override fun call(): Int {
        val fact = client.findFact(storeName, factId)
        fact.printSingle(outputFormat)
        return ExitCode.OK
    }

}