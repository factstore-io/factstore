package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import io.factstore.cli.command.StoreScopedCommand
import io.factstore.cli.command.continueAfterOption
import io.factstore.cli.command.directionOption
import io.factstore.cli.command.limitOption
import io.factstore.cli.command.outputOption
import io.factstore.cli.command.tagOption
import io.factstore.cli.connection.FactStoreClients
import io.factstore.cli.output.writeTo
import io.factstore.client.model.Fact
import io.factstore.client.model.FactFilter
import jakarta.enterprise.context.Dependent
import kotlinx.coroutines.flow.Flow

/** `factstore fact stream` */
@Dependent
class StreamFactsCommand(clients: FactStoreClients) : StoreScopedCommand(name = "stream", clients = clients) {

    override fun help(context: Context) = "Stream the facts of a store, up to its head"

    override fun helpEpilog(context: Context) =
        "The filters describe one criterion: a fact must match any of the subjects, any of the " +
            "types and all of the tags. For several criteria at once use 'fact query'."

    private val subjects by option("--subject", metavar = "<subject>", help = "A subject to match, repeatable").multiple()
    private val types by option("--type", metavar = "<type>", help = "A type to match exactly, repeatable").multiple()
    private val tags by tagOption("A tag a fact must carry, as key=value, repeatable")
    private val continueAfter by continueAfterOption()
    private val direction by directionOption()
    private val limit by limitOption()
    private val output by outputOption()

    override suspend fun run() {
        facts().writeTo(terminal, output)
    }

    /** Without a criterion the whole store is read; any criteria form one filter. */
    private fun facts(): Flow<Fact> = with(client.facts) {
        if (subjects.isEmpty() && types.isEmpty() && tags.isEmpty()) {
            streamFacts(storeName, direction, limit, continueAfter)
        } else {
            streamFactsByQuery(
                storeName = storeName,
                filters = listOf(FactFilter(subjects = subjects, types = types, tags = tags.toTagFilter())),
                direction = direction,
                limit = limit,
                continueAfter = continueAfter,
            )
        }
    }
}
