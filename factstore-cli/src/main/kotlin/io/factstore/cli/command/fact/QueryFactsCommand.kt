package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.file
import io.factstore.cli.command.StoreScopedCommand
import io.factstore.cli.command.continueAfterOption
import io.factstore.cli.command.directionOption
import io.factstore.cli.command.limitOption
import io.factstore.cli.command.outputOption
import io.factstore.cli.command.usageError
import io.factstore.cli.connection.FactStoreClients
import io.factstore.cli.output.writeTo
import io.factstore.client.model.FactQuery
import jakarta.enterprise.context.Dependent

/** `factstore fact query` */
@Dependent
class QueryFactsCommand(clients: FactStoreClients) : StoreScopedCommand(name = "query", clients = clients) {

    override fun help(context: Context) = "Stream the facts matching a query, up to the store's head"

    override fun helpEpilog(context: Context) =
        "A fact matches when it matches any of the query's items. For a single item " +
            "'fact stream' is the simpler command."

    private val items by option(
        "--item",
        metavar = "<json>",
        help = """A query item as JSON, repeatable, e.g. '{"types":["OrderPlaced"],"tags":{"region":"eu"}}'""",
    ).convert { it.toFactQueryItem() }.multiple()

    private val queryFile by option(
        "--query-file",
        metavar = "<file>",
        help = """A file holding the whole query as JSON: {"items":[…]}""",
    ).file().convert { it.toFactQueryItems() }

    private val continueAfter by continueAfterOption()
    private val direction by directionOption()
    private val limit by limitOption()
    private val output by outputOption()

    override suspend fun run() {
        client.facts.streamFacts(
            storeName = storeName,
            query = query(),
            direction = direction,
            limit = limit,
            continueAfter = continueAfter,
        ).writeTo(terminal, output)
    }

    private fun query(): FactQuery {
        val query = items + queryFile.orEmpty()
        if (query.isEmpty()) throw usageError("a query needs at least one item: use --item or --query-file")
        return FactQuery(query)
    }
}
