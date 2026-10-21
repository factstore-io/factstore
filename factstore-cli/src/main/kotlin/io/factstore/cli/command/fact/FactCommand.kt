package io.factstore.cli.command.fact

import com.github.ajalt.clikt.command.SuspendingNoOpCliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.subcommands
import jakarta.enterprise.context.Dependent

/** `factstore fact …`: everything that acts on the facts in a store. */
@Dependent
class FactCommand(
    appendFactCommand: AppendFactCommand,
    getFactCommand: GetFactCommand,
    streamFactsCommand: StreamFactsCommand,
    queryFactsCommand: QueryFactsCommand,
    subscribeFactsCommand: SubscribeFactsCommand,
) : SuspendingNoOpCliktCommand(name = "fact") {

    init {
        subcommands(
            appendFactCommand,
            getFactCommand,
            streamFactsCommand,
            queryFactsCommand,
            subscribeFactsCommand,
        )
    }

    override fun help(context: Context) = "Append and read facts"
}
