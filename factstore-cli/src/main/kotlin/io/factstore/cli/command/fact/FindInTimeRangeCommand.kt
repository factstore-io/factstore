package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.print
import io.factstore.cli.converter.FlexibleInstantConverter
import io.factstore.client.FactStoreClient
import io.factstore.client.model.ReadDirection
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.time.Instant
import java.util.concurrent.Callable

@Command(
    name = "find-in-time-range",
    description = ["Find facts within a given range in time"]
)
class FindInTimeRangeCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Option(
        names = ["--store", "-s"],
        required = true,
        description = ["The name of the store to query (env: FACTSTORE_STORE, config: store)"],
    )
    lateinit var storeName: String

    @Option(
        names = ["--since"],
        description = ["Return facts after this point. Accepts ISO instant (e.g. 2024-01-01T00:00:00Z) or relative duration (e.g. 5m, 2h, 1d)"],
        converter = [FlexibleInstantConverter::class]
    )
    var since: Instant? = null

    @Option(
        names = ["--until"],
        description = ["Return facts before this point. Accepts ISO instant (e.g. 2024-01-01T00:00:00Z) or relative duration (e.g. 5m, 2h, 1d)"],
        converter = [FlexibleInstantConverter::class]
    )
    var until: Instant? = null

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
        val resolvedSince = since
        val resolvedUntil = until

        if (resolvedSince == null && resolvedUntil == null) {
            throw CommandLine.ParameterException(
                CommandLine(this@FindInTimeRangeCommand),
                "At least one of --since or --until must be provided"
            )
        }

        val from = resolvedSince ?: Instant.EPOCH
        val to = resolvedUntil ?: Instant.now()

        if (from >= to) {
            throw CommandLine.ParameterException(
                CommandLine(this@FindInTimeRangeCommand),
                "--since ($from) must be before --until ($to)"
            )
        }

        val facts = client.facts.findInTimeRange(
            storeName = storeName,
            from = from,
            to = to,
            limit = limit,
            direction = direction
        )
        facts.print(outputFormat)
        CommandLine.ExitCode.OK
    }
}