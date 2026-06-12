package io.factstore.cli.command.fact

import io.factstore.cli.command.OutputFormat
import io.factstore.cli.command.print
import io.factstore.cli.converter.TagConverter
import io.factstore.client.FactStoreClient
import io.factstore.client.model.ReadDirection
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.util.concurrent.Callable

@Command(
    name = "find-by-tags",
    description = ["Find facts matching one or more tags"]
)
class FindByTagsCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Option(
        names = ["--tag", "-t"],
        required = true,
        arity = "1..*",
        description = ["Tags to filter by in key=value format (repeatable: -t class=high -t region=eu)"],
        paramLabel = "<key=value>",
        converter = [TagConverter::class]
    )
    lateinit var tags: List<Pair<String, String>>

    @Option(
        names = ["--store", "-s"],
        required = true,
        description = ["The name of the store to query (env: FACTSTORE_STORE, config: store)"],
    )
    lateinit var storeName: String

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
        val facts = client.facts.findByTags(
            storeName = storeName,
            tags = tags.toMap(),
            limit = limit,
            direction = direction
        )
        facts.print(outputFormat)
        CommandLine.ExitCode.OK
    }


}