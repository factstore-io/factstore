package io.factstore.cli.command.store

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import io.factstore.cli.command.ServerCommand
import io.factstore.cli.connection.FactStoreClients
import io.factstore.cli.output.toStoreTable
import jakarta.enterprise.context.Dependent

/** `factstore store list` (or `ls`) */
@Dependent
class ListStoresCommand(clients: FactStoreClients) : ServerCommand(name = "list", clients = clients) {

    override fun help(context: Context) = "List all stores"

    override suspend fun run() {
        val stores = client.stores.list()
        if (stores.isEmpty()) {
            echo("No stores found.")
            return
        }
        terminal.println(stores.toStoreTable())
    }
}
