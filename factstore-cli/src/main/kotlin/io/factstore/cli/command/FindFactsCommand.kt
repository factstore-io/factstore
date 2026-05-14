package io.factstore.cli.command

import io.factstore.cli.client.FactHttp
import io.factstore.cli.client.FactStoreClient
import io.factstore.cli.converter.FlexibleInstantConverter
import jakarta.inject.Inject
import picocli.CommandLine.*
import java.time.Instant
import java.util.concurrent.Callable

@Command(name = "facts")
class FindFactsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Option(
        names = ["--store", "-s"],
        description = ["The name of the store to query"],
        required = true,
    )
    lateinit var storeName: String

    @ArgGroup(
        exclusive = true,
        heading = "Filter options (mutually exclusive):%n",
    )
    var filter: FactFilter? = null

    @Option(
        names = ["--limit"],
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

    override fun call(): Int {
        val facts = filter.toClientCall()

        if (facts.size >= limit) {
            System.err.println("Warning: results limited to $limit facts. Use --limit to retrieve more.")
        }

        facts.print(outputFormat)
        return ExitCode.OK
    }

    private fun FactFilter?.toClientCall(): List<FactHttp> = when {
        this == null -> client.findFacts(storeName = storeName, limit = limit, direction = direction.name)
        subject != null -> client.findFactsBySubject(
            storeName = storeName,
            subject = subject!!,
            limit = limit,
            direction = direction.name,
        )
        tags.isNotEmpty() -> client.findFacts(
            storeName = storeName,
            tags = tags,
            limit = limit,
            direction = direction.name,
        )
        else -> {
            val range = timeRange
            if (range != null) client.findFacts(
                storeName = storeName,
                from = range.since,
                to = range.until,
                limit = limit,
                direction = direction.name,
            ) else client.findFacts(
                storeName = storeName,
                limit = limit,
                direction = direction.name,
            )
        }
    }
}

enum class ReadDirection {
    forward, backward;
}

class FactFilter {

    @Option(
        names = ["--subject"],
        description = ["Filter by subject (e.g. order/123)"]
    )
    var subject: String? = null

    @Option(
        names = ["--tag"],
        description = ["Filter by tag in key=value format (e.g. env=prod). Repeatable — all tags must match (AND semantics)."],
    )
    var tags: List<String> = emptyList()

    @ArgGroup(exclusive = false)
    var timeRange: TimeRangeFilter? = null
}

class TimeRangeFilter {

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
}