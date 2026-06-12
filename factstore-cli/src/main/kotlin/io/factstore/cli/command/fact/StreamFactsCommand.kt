package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.printSingle
import io.factstore.cli.config.FactStoreConfigResolver
import io.factstore.client.FactStoreClient
import io.factstore.client.model.StreamStartPosition
import io.smallrye.mutiny.coroutines.asFlow
import io.vertx.core.http.HttpClosedException
import jakarta.inject.Inject
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import java.util.UUID
import java.util.concurrent.Callable
import kotlin.coroutines.cancellation.CancellationException

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
            .catch { cause -> cause.handleStreamTermination() }
            .collect { fact -> fact.printSingle(outputFormat) }

        CommandLine.ExitCode.OK
    }

    private fun Throwable.handleStreamTermination() {
        val underlying = (this as? CancellationException)?.cause ?: this
        when (underlying) {
            is HttpClosedException -> return  // normal termination (user Ctrl+C or server closed stream)
            else -> throw underlying
        }
    }

}