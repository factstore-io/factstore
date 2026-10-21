package io.factstore.cli.command.store

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import io.factstore.cli.command.ServerCommand
import io.factstore.cli.command.outputOption
import io.factstore.cli.connection.FactStoreClients
import io.factstore.cli.output.writeTo
import jakarta.enterprise.context.Dependent

/** `factstore store list` (or `ls`) */
@Dependent
class ListStoresCommand(clients: FactStoreClients) : ServerCommand(name = "list", clients = clients) {

    override fun help(context: Context) = "List all stores"

    private val output by outputOption()

    override suspend fun run() = client.stores.list().writeTo(terminal, output)
}
