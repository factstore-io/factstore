package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.convert
import io.factstore.cli.command.StoreScopedCommand
import io.factstore.cli.command.factId
import io.factstore.cli.command.outputOption
import io.factstore.cli.connection.FactStoreClients
import io.factstore.cli.output.writeTo
import jakarta.enterprise.context.Dependent

/** `factstore fact get <factId>` */
@Dependent
class GetFactCommand(clients: FactStoreClients) : StoreScopedCommand(name = "get", clients = clients) {

    override fun help(context: Context) = "Get a single fact by its id"

    private val factId by argument("factId", help = "The id of the fact to get").convert { factId(it) }
    private val output by outputOption()

    override suspend fun run() {
        client.facts.get(storeName, factId).writeTo(terminal, output)
    }
}
