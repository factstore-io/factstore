package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.print
import io.factstore.cli.exception.CliUsageException
import io.factstore.client.FactStoreClient
import io.factstore.client.model.FactFilter
import io.factstore.client.model.ReadDirection
import jakarta.inject.Inject
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.io.File
import java.util.concurrent.Callable

@Command(
    name = "query",
    description = [
        "Find facts matching a query: a fact matches when it matches any filter.",
        "The options --subject, --type and --tag build one filter; --filter adds filters as JSON.",
    ]
)
class QueryFactsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Option(
        names = ["--store", "-s"],
        required = true,
        description = ["The name of the store to query (env: FACTSTORE_STORE, config: store)"],
    )
    lateinit var storeName: String

    @Option(
        names = ["--subject"],
        description = ["A subject to match, repeatable; the fact's subject must be one of them"],
    )
    var subjects: MutableList<String> = mutableListOf()

    @Option(
        names = ["--type"],
        description = ["A type to match, repeatable; the fact's type must be one of them"],
    )
    var types: MutableList<String> = mutableListOf()

    @Option(
        names = ["--tag"],
        description = ["A tag the fact must carry, as key=value, repeatable; all must match"],
    )
    var tags: MutableList<String> = mutableListOf()

    @Option(
        names = ["--filter"],
        description = ["A filter as JSON, repeatable, e.g. '{\"types\":[\"OrderPlaced\"],\"tags\":{\"region\":\"eu\"}}'"],
    )
    var filters: MutableList<String> = mutableListOf()

    @Option(
        names = ["--query-file"],
        description = ["A file holding the query as JSON: {\"filters\":[…]}"],
    )
    var queryFile: File? = null

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

    override fun call(): Int = runBlocking {
        val filters = collectFilters()
        if (filters.isEmpty()) {
            throw CliUsageException(
                "A query needs at least one filter: use --subject, --type, --tag, --filter or --query-file."
            )
        }

        // Collected in full: the table needs every row to size its columns, and --limit bounds it.
        val facts = client.facts.streamFactsByQuery(
            storeName = storeName,
            filters = filters,
            direction = direction,
            limit = limit,
        ).toList()

        facts.print(outputFormat)

        CommandLine.ExitCode.OK
    }

    private fun collectFilters(): List<FactFilter> = buildList {
        // The convenience options describe one filter.
        if (subjects.isNotEmpty() || types.isNotEmpty() || tags.isNotEmpty()) {
            add(FactFilter(subjects = subjects, types = types, tags = tags.toTagMap()))
        }
        filters.forEach { add(it.toFactFilter()) }
        queryFile?.let { file ->
            val query = json.parseToJsonElement(file.readText()).jsonObject
            val filters = query["filters"] ?: throw CliUsageException("The query file has no 'filters'.")
            filters.jsonArray.forEach { add(it.toString().toFactFilter()) }
        }
    }

    private fun List<String>.toTagMap(): Map<String, String> = associate { tag ->
        val parts = tag.split("=", limit = 2)
        if (parts.size != 2) throw CliUsageException("A tag must have the form key=value, but was '$tag'.")
        parts[0] to parts[1]
    }

    private fun String.toFactFilter(): FactFilter {
        val filter = try {
            json.parseToJsonElement(this).jsonObject
        } catch (e: Exception) {
            throw CliUsageException("A filter must be a JSON object, but was '$this': ${e.message}")
        }
        return FactFilter(
            subjects = filter["subjects"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList(),
            types = filter["types"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList(),
            tags = filter["tags"]?.jsonObject?.mapValues { it.value.jsonPrimitive.content } ?: emptyMap(),
        )
    }

    private companion object {
        val json = Json { ignoreUnknownKeys = true }
    }
}
