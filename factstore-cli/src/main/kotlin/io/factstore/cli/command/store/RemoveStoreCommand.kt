package io.factstore.cli.command.store

import io.factstore.client.FactStoreClient
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine.Command
import picocli.CommandLine.Parameters

@Command(
    name = "remove",
    aliases = [ "rm" ],
    description = ["Remove a store"]
)
class RemoveStoreCommand : Runnable {

    @Inject
    lateinit var client: FactStoreClient

    @Parameters(
        index = "0",
        arity = "1",
        description = ["The name of the store"]
    )
    lateinit var storeName: String

    override fun run() = runBlocking {
        client.stores.delete(storeName)
        println("✅ Store '$storeName' removed successfully.")
    }
}
