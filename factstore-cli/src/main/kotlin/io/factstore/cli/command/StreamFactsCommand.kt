package io.factstore.cli.command

import io.factstore.cli.client.FactStoreClient
import io.smallrye.mutiny.coroutines.asFlow
import io.vertx.core.http.HttpClosedException
import jakarta.inject.Inject
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.runBlocking
import picocli.CommandLine.*
import java.util.*
import java.util.concurrent.Callable
import kotlin.coroutines.cancellation.CancellationException

@Command(
    name = "stream",
    description = ["Stream facts from a store in real-time (similar to tail -f)"]
)
class StreamFactsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Parameters(
        index = "0",
        arity = "1",
        description = ["The name of the store to stream from"]
    )
    lateinit var storeName: String

    @ArgGroup(
        exclusive = true,
        heading = "Start position options:%n"
    )
    var startPosition = StartPosition()

    class StartPosition {

        @Option(
            names = ["--from"],
            description = ["Start from: 'beginning' or 'end'"],
            showDefaultValue = Help.Visibility.ALWAYS,
        )
        var from: FromOption? = null

        @Option(
            names = ["--after"],
            description = ["Start after a specific Fact ID (UUID)"]
        )
        var after: UUID? = null

    }

    @Option(
        names = ["--output", "-o"],
        description = ["Output format (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    enum class FromOption { beginning, end }

    override fun call(): Int = runBlocking {
        val fromValue = startPosition.from?.name
        val afterValue = startPosition.after

        client.streamFacts(storeName, fromValue, afterValue)
            .asFlow()
            .catch { cause -> cause.handleStreamTermination() }
            .collect { fact -> fact.printSingle(outputFormat) }

        ExitCode.OK
    }

    private fun Throwable.handleStreamTermination() {
        val underlying = (this as? CancellationException)?.cause ?: this
        when (underlying) {
            is HttpClosedException -> return  // normal termination (user Ctrl+C or server closed stream)
            else -> throw underlying
        }
    }

}
