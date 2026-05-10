package io.factstore.cli.command

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
        description = [$$"Maximum number of facts to return (default: ${DEFAULT-VALUE})"],
        defaultValue = "100",
    )
    var limit: Int = 100

    @Option(
        names = ["--reversed"],
        description = ["Return facts in reverse chronological order (newest first)"]
    )
    var reversed: Boolean = false

    override fun call(): Int {
        val subject = filter?.subject
        val timeRange = filter?.timeRange

        val facts = when {
            subject != null -> client.findFactsBySubject(storeName, subject)
            timeRange != null -> client.findFactsInTimeRange(
                storeName = storeName,
                from = timeRange.since,
                to = timeRange.until,
            )
            else -> client.findFactsInTimeRange(
                storeName = storeName,
                from = null,
                to = null,
            )
        }

        if (facts.size >= limit) {
            System.err.println("Warning: results limited to $limit facts. Use --limit to retrieve more.")
        }

        printTable(facts)
        return ExitCode.OK
    }

}

class FactFilter {

    @Option(
        names = ["--subject"],
        description = ["Filter by subject (e.g. order/123)"]
    )
    var subject: String? = null

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
