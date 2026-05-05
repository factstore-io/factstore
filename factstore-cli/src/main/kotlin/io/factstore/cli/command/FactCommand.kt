package io.factstore.cli.command

import picocli.CommandLine.Command

@Command(
    name = "fact",
    aliases = [ "facts" ],
    description = ["Operations around facts"],
    subcommands = [
        AppendFactCommand::class,
        StreamFactsCommand::class,
    ]
)
class FactCommand
