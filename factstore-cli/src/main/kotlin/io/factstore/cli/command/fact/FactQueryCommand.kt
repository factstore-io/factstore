package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.printSingle
import io.factstore.cli.config.FactStoreConfigResolver
import io.factstore.cli.converter.FlexibleInstantConverter
import io.factstore.cli.converter.TagConverter
import io.factstore.client.FactStoreClient
import io.factstore.client.model.FactFilter
import io.factstore.client.model.ReadDirection
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.io.File
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Callable

/**
 * Executes a composable [FactFilter] query via [FactStoreClient.facts]' `streamFacts`.
 *
 * Two ways to express the filter:
 * - **Flags** (the common path): `--subject`, `--subject-prefix`, `--type`, `--tag`,
 *   `--metadata`, `--since`/`--until` all AND together at the root, mirroring
 *   `FactFilter.AllOf` of leaves.
 * - **JSON** (`--filter`/`--filter-file`), for anything the flat AND model can't express
 *   — multiple types via OR, nested `anyOf`/`allOf`, DCB-style patterns. The JSON shape
 *   matches the HTTP API's `FactFilterHttp` discriminators exactly, so a body written
 *   for one works for the other.
 *
 * `--last`/`--first` wrap whichever filter results (flag-built or JSON) in a bounded
 * selector and are therefore orthogonal to which filter source is used.
 */
@Command(
    name = "query",
    description = ["Run a composable query across subject, type, tags, metadata, and time range"]
)
class FactQueryCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Inject
    lateinit var configResolver: FactStoreConfigResolver

    @Option(
        names = ["--store", "-s"],
        description = ["The name of the store to query (env: FACTSTORE_STORE, config: store)"],
    )
    var storeName: String? = null

    // ── Flag-composed filter (AND semantics at the root) ────────────────────────

    @Option(names = ["--subject"], description = ["Match facts whose subject equals this value"])
    var subject: String? = null

    @Option(names = ["--subject-prefix"], description = ["Match facts whose subject starts with this prefix"])
    var subjectPrefix: String? = null

    @Option(names = ["--type"], description = ["Match facts whose type equals this value"])
    var type: String? = null

    @Option(
        names = ["--tag", "-t"],
        arity = "1..*",
        description = ["Tag to filter by in key=value format (repeatable, AND semantics: -t region=eu -t env=prod)"],
        paramLabel = "<key=value>",
        converter = [TagConverter::class],
    )
    var tags: List<Pair<String, String>> = emptyList()

    @Option(
        names = ["--metadata"],
        arity = "1..*",
        description = ["Metadata entry to filter by in key=value format (repeatable, AND semantics)"],
        paramLabel = "<key=value>",
        converter = [TagConverter::class],
    )
    var metadata: List<Pair<String, String>> = emptyList()

    @Option(
        names = ["--since"],
        description = ["Match facts appended after this point. ISO instant (e.g. 2024-01-01T00:00:00Z) or relative duration (e.g. 5m, 2h, 1d)"],
        converter = [FlexibleInstantConverter::class],
    )
    var since: Instant? = null

    @Option(
        names = ["--until"],
        description = ["Match facts appended before this point. ISO instant or relative duration"],
        converter = [FlexibleInstantConverter::class],
    )
    var until: Instant? = null

    // ── JSON escape hatch, for anything flags can't express (OR, nested trees) ──

    @Option(
        names = ["--filter"],
        description = ["A FactFilter as a JSON string, for OR/nested trees the flags above can't express. Mutually exclusive with the flag-based filter options and --filter-file"],
        paramLabel = "<json>",
    )
    var filterJson: String? = null

    @Option(
        names = ["--filter-file"],
        description = ["A FactFilter as a JSON file, same shape as --filter"],
        paramLabel = "<path>",
    )
    var filterFile: File? = null

    // ── Bounded selector — orthogonal to filter source, wraps whichever is used ─

    @Option(names = ["--last"], description = ["From the matching facts, select only the last N (most recent)"], paramLabel = "<n>")
    var last: Int? = null

    @Option(names = ["--first"], description = ["From the matching facts, select only the first N (oldest)"], paramLabel = "<n>")
    var first: Int? = null

    // ── Query mechanics ──────────────────────────────────────────────────────────

    @Option(
        names = ["--limit"],
        description = ["Maximum number of facts to return"],
    )
    var limit: Int? = null

    @Option(
        names = ["--direction"],
        description = ["Read direction: \${COMPLETION-CANDIDATES} (default: \${DEFAULT-VALUE})"],
        defaultValue = "forward",
    )
    lateinit var direction: ReadDirection

    @Option(
        names = ["--cursor"],
        description = ["Start strictly after this Fact ID (UUID) — for paging through results"],
    )
    var cursor: UUID? = null

    @Option(
        names = ["--output", "-o"],
        description = ["Output format: \${COMPLETION-CANDIDATES} (default: \${DEFAULT-VALUE})"],
        defaultValue = "table",
    )
    var outputFormat: OutputFormat = OutputFormat.Table

    private val json = Json { ignoreUnknownKeys = false }

    override fun call(): Int = runBlocking {
        val storeName = configResolver.resolveStore(storeName)

        val hasFlagFilter = subject != null || subjectPrefix != null || type != null ||
            tags.isNotEmpty() || metadata.isNotEmpty() || since != null || until != null
        val hasJsonFilter = filterJson != null || filterFile != null

        if (filterJson != null && filterFile != null) {
            fail("Use either --filter or --filter-file, not both")
        }
        if (hasFlagFilter && hasJsonFilter) {
            fail("--filter/--filter-file cannot be combined with --subject/--subject-prefix/--type/--tag/--metadata/--since/--until")
        }
        if (last != null && first != null) {
            fail("--last and --first cannot be combined")
        }
        if ((last != null || first != null) && !hasFlagFilter && !hasJsonFilter) {
            fail(
                "--last/--first require at least one filter (--subject, --type, --tag, --metadata, " +
                    "--since/--until, or --filter/--filter-file) to bound. For \"last N facts overall\" " +
                    "with no filter, use --limit N --direction backward instead."
            )
        }

        val baseFilter: FactFilter = if (hasJsonFilter) parseJsonFilter() else buildFlagFilter()
        val filter = when {
            last != null -> FactFilter.Last(last!!, baseFilter)
            first != null -> FactFilter.First(first!!, baseFilter)
            else -> baseFilter
        }

        client.facts.streamFacts(
            storeName = storeName,
            filter = filter,
            limit = limit,
            direction = direction,
            cursor = cursor?.toString(),
        ).collect { fact -> fact.printSingle(outputFormat) }

        CommandLine.ExitCode.OK
    }

    private fun buildFlagFilter(): FactFilter {
        val leaves = buildList {
            subject?.let { add(FactFilter.Subject(it)) }
            subjectPrefix?.let { add(FactFilter.SubjectPrefix(it)) }
            type?.let { add(FactFilter.Type(it)) }
            tags.forEach { (key, value) -> add(FactFilter.Tag(key, value)) }
            metadata.forEach { (key, value) -> add(FactFilter.Metadata(key, value)) }
            if (since != null || until != null) add(FactFilter.TimeRange(since, until))
        }
        return when (leaves.size) {
            0 -> FactFilter.All
            1 -> leaves.single()
            else -> FactFilter.AllOf(leaves)
        }
    }

    private fun parseJsonFilter(): FactFilter {
        val text = filterJson ?: filterFile!!.let { file ->
            if (!file.exists()) fail("Filter file not found: $file")
            file.readText()
        }
        return try {
            json.decodeFromString(FactFilter.serializer(), text)
        } catch (e: Exception) {
            fail("Invalid --filter JSON: ${e.message}")
        }
    }

    private fun fail(message: String): Nothing =
        throw CommandLine.ParameterException(CommandLine(this), message)

}
