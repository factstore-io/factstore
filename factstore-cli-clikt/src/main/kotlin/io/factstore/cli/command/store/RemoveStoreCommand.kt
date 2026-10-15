package io.factstore.cli.command.store

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.parameters.arguments.argument
import io.factstore.cli.command.ServerCommand
import io.factstore.cli.connection.FactStoreClients
import jakarta.enterprise.context.Dependent

/** `factstore store remove <name>` (or `rm`) */
@Dependent
class RemoveStoreCommand(clients: FactStoreClients) : ServerCommand(name = "remove", clients = clients) {

    override fun help(context: Context) = "Remove a store and every fact in it"

    private val storeName by argument("name", help = "The name of the store")

    override suspend fun run() {
        client.stores.delete(storeName)
        echo("✅ Store '$storeName' removed.")
    }
}
