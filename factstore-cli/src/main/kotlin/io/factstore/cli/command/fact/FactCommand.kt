package io.factstore.cli.command.fact

import picocli.CommandLine.Command

@Command(
    name = "fact",
    aliases = [ "facts" ],
    description = ["Operations around facts"],
    subcommands = [
        AppendFactCommand::class,
        StreamFactsCommand::class,
        FindBySubjectCommand::class,
        FindByTagsCommand::class,
        FindInTimeRangeCommand::class,
        FindByIdCommand::class,
    ]
)
class FactCommand