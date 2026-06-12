package io.factstore.cli.command.store

import picocli.CommandLine

@CommandLine.Command(
    name = "store",
    aliases = [ "stores" ],
    description = ["Manage stores"],
    subcommands = [
        CreateStoreCommand::class,
        ListStoresCommand::class,
        RemoveStoreCommand::class,
    ]
)
class StoreCommand