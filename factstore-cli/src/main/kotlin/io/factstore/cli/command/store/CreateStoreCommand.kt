package io.factstore.cli.command.store

import io.factstore.client.FactStoreClient
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine.*
import java.util.concurrent.Callable

@Command(
    name = "create",
    description = ["Create a new logical store"]
)
class CreateStoreCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Parameters(
        index = "0",
        arity = "1",
        description = ["The unique name of the store"]
    )
    lateinit var storeName: String

    override fun call(): Int = runBlocking {
        client.stores.create(storeName)
        println("✅ Store '$storeName' created successfully.")
        ExitCode.OK
    }
}
