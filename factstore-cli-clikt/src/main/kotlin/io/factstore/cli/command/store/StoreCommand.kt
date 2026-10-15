package io.factstore.cli.command.store

import com.github.ajalt.clikt.command.SuspendingNoOpCliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.subcommands
import jakarta.enterprise.context.Dependent

/** `factstore store …`: everything that acts on stores rather than on facts. */
@Dependent
class StoreCommand(
    createStoreCommand: CreateStoreCommand,
    listStoresCommand: ListStoresCommand,
    removeStoreCommand: RemoveStoreCommand,
) : SuspendingNoOpCliktCommand(name = "store") {

    init {
        subcommands(createStoreCommand, listStoresCommand, removeStoreCommand)
    }

    override fun help(context: Context) = "Manage stores"

    // Clikt resolves aliases on the parent of the command they stand for.
    override fun aliases() = mapOf(
        "ls" to listOf("list"),
        "rm" to listOf("remove"),
    )
}
