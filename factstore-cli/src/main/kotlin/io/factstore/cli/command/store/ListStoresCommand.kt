package io.factstore.cli.command.store

import io.factstore.client.FactStoreClient
import io.factstore.client.model.StoreInfo
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine.Command
import java.time.LocalDateTime
import java.time.ZoneOffset

@Command(
    name = "list",
    aliases = [ "ls" ],
    description = ["List all available logical stores"]
)
class ListStoresCommand : Runnable {

    @Inject
    lateinit var client: FactStoreClient

    override fun run() = runBlocking {
        val metadata = client.stores.list()

        if (metadata.isEmpty()) {
            println("No stores found.")
        } else {
            metadata.print()
        }
    }

    private fun List<StoreInfo>.print() {
        // Table Header
        println("%-25s %-25s %-36s".format("NAME", "CREATED AT", "STORE ID"))
        println("-".repeat(90))

        // Table Rows
        forEach { store ->
            println("%-25s %-25s %-36s".format(
                store.name,
                LocalDateTime.ofInstant(store.createdAt, ZoneOffset.systemDefault()),
                store.id
            ))
        }
    }
}
