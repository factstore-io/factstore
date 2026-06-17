package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.printSingle
import io.factstore.cli.config.FactStoreConfigResolver
import io.factstore.client.FactStoreClient
import io.factstore.client.model.StreamStartPosition
import jakarta.inject.Inject
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import java.util.*
import java.util.concurrent.Callable

@CommandLine.Command(
    name = "stream",
    description = ["Stream facts from a store in real-time (similar to tail -f)"]
)
class StreamFactsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Inject
    lateinit var configResolver: FactStoreConfigResolver

    @CommandLine.Option(
        names = ["--store", "-s"],
        description = ["The name of the store to stream from (env: FACTSTORE_STORE, config: store)"],
    )
    var storeName: String? = null

    @CommandLine.ArgGroup(
        exclusive = true,
        heading = "Start position options:%n"
    )
    var startPosition = StartPosition()

    class StartPosition {

        @CommandLine.Option(
            names = ["--from"],
            description = ["Start from: 'beginning' or 'end'"],
            showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
        )
        var from: FromOption? = null

        @CommandLine.Option(
            names = ["--after"],
            description = ["Start after a specific Fact ID (UUID)"]
        )
        var after: UUID? = null

    }

    @CommandLine.Option(
        names = ["--output", "-o"],
        description = ["Output format (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    enum class FromOption { beginning, end }

    override fun call(): Int = runBlocking {
        val storeName = configResolver.resolveStore(storeName)
        val fromValue = startPosition.from
        val afterValue = startPosition.after

        val streamStartPosition = afterValue?.let { StreamStartPosition.AfterFact(afterValue.toString()) }
            ?: fromValue?.let { if (fromValue == FromOption.beginning) StreamStartPosition.Beginning else StreamStartPosition.End }
            ?: StreamStartPosition.Beginning

        client.facts.stream(storeName, streamStartPosition)
            .catch { cause -> if (!jvmIsShuttingDown()) throw cause }
            .collect { fact -> fact.printSingle(outputFormat) }

        CommandLine.ExitCode.OK
    }

    // Ctrl+C triggers JVM shutdown, during which Quarkus closes the gRPC channel and the in-flight
    // stream fails. That's the user asking to quit, not an error, so terminate quietly. The JVM
    // rejects (de)registering shutdown hooks once shutdown is in progress; use that to detect it.
    // Reliable here because the channel is closed from within a shutdown hook, so shutdown is
    // already in progress by the time the failure reaches us.
    private fun jvmIsShuttingDown(): Boolean =
        runCatching {
            val probe = Thread {}
            Runtime.getRuntime().addShutdownHook(probe)
            Runtime.getRuntime().removeShutdownHook(probe)
        }.isFailure

}
