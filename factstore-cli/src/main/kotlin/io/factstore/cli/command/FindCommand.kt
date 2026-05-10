package io.factstore.cli.command

import picocli.CommandLine.Command

@Command(
    name = "find",
    description = ["Find facts in a store"],
    subcommands = [
        FindFactCommand::class,
        FindFactsCommand::class,
    ]
)
class FindCommand
