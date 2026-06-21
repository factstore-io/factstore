package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.printSingle
import io.factstore.cli.config.FactStoreConfigResolver
import io.factstore.client.FactStoreClient
import io.factstore.client.model.ReplayStartPosition
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import java.util.*
import java.util.concurrent.Callable

@CommandLine.Command(
    name = "replay",
    description = ["Replay facts from a store up to the current head, then exit"]
)
class ReplayFactsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Inject
    lateinit var configResolver: FactStoreConfigResolver

    @CommandLine.Option(
        names = ["--store", "-s"],
        description = ["The name of the store to replay from (env: FACTSTORE_STORE, config: store)"],
    )
    var storeName: String? = null

    @CommandLine.Option(
        names = ["--after"],
        description = ["Replay only facts after a specific Fact ID (UUID); defaults to the beginning"]
    )
    var after: UUID? = null

    @CommandLine.Option(
        names = ["--output", "-o"],
        description = ["Output format (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    override fun call(): Int = runBlocking {
        val storeName = configResolver.resolveStore(storeName)

        val start = after?.let { ReplayStartPosition.AfterFact(it.toString()) }
            ?: ReplayStartPosition.Beginning

        client.facts.replay(storeName, start)
            .collect { fact -> fact.printSingle(outputFormat) }

        CommandLine.ExitCode.OK
    }

}
