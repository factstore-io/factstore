package io.factstore.cli.command.fact

import picocli.CommandLine.Command

@Command(
    name = "fact",
    aliases = [ "facts" ],
    description = ["Operations around facts"],
    subcommands = [
        AppendFactCommand::class,
        GetFactCommand::class,
        StreamFactsCommand::class,
        QueryFactsCommand::class,
        SubscribeFactsCommand::class,
    ]
)
class FactCommand