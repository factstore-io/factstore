package io.factstore.cli.command.fact

import picocli.CommandLine.Command

@Command(
    name = "fact",
    aliases = [ "facts" ],
    description = ["Operations around facts"],
    subcommands = [
        AppendFactCommand::class,
        SubscribeFactsCommand::class,
        ReplayFactsCommand::class,
        FindBySubjectCommand::class,
        FindByTypeCommand::class,
        FindByTagsCommand::class,
        QueryFactsCommand::class,
        FindByIdCommand::class,
    ]
)
class FactCommand