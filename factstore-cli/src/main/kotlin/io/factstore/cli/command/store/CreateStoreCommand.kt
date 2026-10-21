package io.factstore.cli.command.store

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.parameters.arguments.argument
import io.factstore.cli.command.ServerCommand
import io.factstore.cli.connection.FactStoreClients
import jakarta.enterprise.context.Dependent

/** `factstore store create <name>` */
@Dependent
class CreateStoreCommand(clients: FactStoreClients) : ServerCommand(name = "create", clients = clients) {

    override fun help(context: Context) = "Create a new store"

    private val storeName by argument("name", help = "The unique name of the store")

    override suspend fun run() {
        val storeId = client.stores.create(storeName)
        echo("✅ Store '$storeName' created. Id: $storeId")
    }
}
