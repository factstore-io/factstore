package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.print
import io.factstore.cli.converter.TagConverter
import io.factstore.client.FactStoreClient
import io.factstore.client.model.Fact
import io.factstore.client.model.FactFilter
import io.factstore.client.model.ReadDirection
import jakarta.inject.Inject
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.util.UUID
import java.util.concurrent.Callable

@Command(
    name = "stream",
    description = [
        "Stream the facts of a store, optionally filtered by subject, type and tags.",
        "The filters describe one criterion: a fact must match any of the subjects, any of the",
        "types and all of the tags. For several criteria at once use 'fact query'.",
    ]
)
class StreamFactsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Option(
        names = ["--store", "-s"],
        required = true,
        description = ["The name of the store to read from (env: FACTSTORE_STORE, config: store)"],
    )
    lateinit var storeName: String

    @Option(
        names = ["--subject"],
        description = ["A subject to match, repeatable; the fact's subject must be one of them"],
    )
    var subjects: MutableList<String> = mutableListOf()

    @Option(
        names = ["--type"],
        description = ["A type to match exactly, repeatable; the fact's type must be one of them"],
    )
    var types: MutableList<String> = mutableListOf()

    @Option(
        names = ["--tag", "-t"],
        description = ["A tag the fact must carry, as key=value, repeatable; all of them must match"],
        paramLabel = "<key=value>",
        converter = [TagConverter::class],
    )
    var tags: MutableList<Pair<String, String>> = mutableListOf()

    @Option(
        names = ["--continue-after"],
        description = ["Continue after this fact, exclusive and in reading order"],
        paramLabel = "<factId>",
    )
    var continueAfter: UUID? = null

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
        description = ["Output format: \${COMPLETION-CANDIDATES} (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    override fun call(): Int = runBlocking {
        facts().print(outputFormat)
        CommandLine.ExitCode.OK
    }

    // The cheapest read that can serve the options is chosen, as the HTTP API does: a single
    // subject, a single type and tags alone each have an index of their own, while anything
    // combined is a query of one filter.
    private fun facts(): Flow<Fact> {
        val after = continueAfter?.toString()
        return when {
            subjects.isEmpty() && types.isEmpty() && tags.isEmpty() ->
                client.facts.streamFacts(storeName, direction, limit, after)

            subjects.size == 1 && types.isEmpty() && tags.isEmpty() ->
                client.facts.streamFactsBySubject(storeName, subjects.single(), direction, limit, after)

            types.size == 1 && subjects.isEmpty() && tags.isEmpty() ->
                client.facts.streamFactsByType(storeName, types.single(), direction, limit, after)

            subjects.isEmpty() && types.isEmpty() ->
                client.facts.streamFactsByTags(storeName, tags.toMap(), direction, limit, after)

            else -> client.facts.streamFactsByQuery(
                storeName = storeName,
                filters = listOf(FactFilter(subjects = subjects, types = types, tags = tags.toMap())),
                direction = direction,
                limit = limit,
                continueAfter = after,
            )
        }
    }
}
