package io.factstore.cli.command

import picocli.CommandLine.Command

@Command(
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
