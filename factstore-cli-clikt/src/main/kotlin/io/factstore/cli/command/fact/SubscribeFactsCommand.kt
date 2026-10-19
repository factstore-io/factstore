package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.groups.default
import com.github.ajalt.clikt.parameters.groups.mutuallyExclusiveOptions
import com.github.ajalt.clikt.parameters.groups.provideDelegate
import com.github.ajalt.clikt.parameters.groups.single
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.enum
import io.factstore.cli.command.StoreScopedCommand
import io.factstore.cli.command.factId
import io.factstore.cli.command.outputOption
import io.factstore.cli.connection.FactStoreClients
import io.factstore.cli.output.OutputFormat
import io.factstore.cli.output.followTo
import io.factstore.client.model.SubscribeStartPosition
import jakarta.enterprise.context.Dependent

/** Where a subscription starts, as the command line says it. */
private enum class From(val position: SubscribeStartPosition) {
    Beginning(SubscribeStartPosition.Beginning),
    End(SubscribeStartPosition.End),
}

/** `factstore fact subscribe` */
@Dependent
class SubscribeFactsCommand(clients: FactStoreClients) : StoreScopedCommand(name = "subscribe", clients = clients) {

    override fun help(context: Context) = "Follow a store's facts as they are appended, like tail -f"

    override fun helpEpilog(context: Context) =
        "Every fact of the store is followed; subscriptions take no filters yet. " +
            "Press Ctrl+C to stop."

    /**
     * Where to start, which is one choice: from an end of the store, or after a known fact.
     *
     * The default is the store's end, because a subscription is `tail -f` and `tail -f` does not
     * print the whole file first. Pass `--from beginning` for a deliberate replay.
     */
    private val startPosition by mutuallyExclusiveOptions(
        option("--from", help = "Start at the store's beginning or its end")
            .enum<From>(key = { it.name.lowercase() })
            .convert { it.position },
        option("--continue-after", metavar = "<factId>", help = "Continue after a known fact, exclusive")
            .convert { SubscribeStartPosition.ContinueAfter(factId(it)) },
        name = "Start position",
    ).single().default(SubscribeStartPosition.End)

    private val output by outputOption(OutputFormat.Table, OutputFormat.Ndjson)

    override suspend fun run() {
        client.facts.subscribe(storeName, startPosition).followTo(terminal, output)
    }
}
