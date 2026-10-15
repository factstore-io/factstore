package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.optional
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import io.factstore.cli.command.StoreScopedCommand
import io.factstore.cli.command.tagOption
import io.factstore.cli.connection.FactStoreClients
import io.factstore.client.model.AppendOutcome
import io.factstore.client.model.FactInput
import io.factstore.client.model.FactPayload
import jakarta.enterprise.context.Dependent
import java.util.UUID

/** `factstore fact append <payload>` */
@Dependent
class AppendFactCommand(clients: FactStoreClients) : StoreScopedCommand(name = "append", clients = clients) {

    override fun help(context: Context) = "Append a fact to a store"

    private val type by option("--type", metavar = "<type>", help = "What happened, e.g. ORDER_PLACED").required()
    private val subject by option("--subject", metavar = "<subject>", help = "The entity it happened to, e.g. order/12345").required()
    private val tags by tagOption("A tag to carry, as key=value, repeatable")
    private val payload by argument("payload", help = "The fact's payload; read from stdin when absent").optional()

    override suspend fun run() {
        val outcome = client.facts.append(
            storeName = storeName,
            facts = listOf(
                FactInput(
                    type = type,
                    subject = subject,
                    payload = FactPayload(payload.orStdin().toByteArray()),
                    tags = tags.toTagFilter(),
                )
            ),
            idempotencyKey = UUID.randomUUID().toString(),
        )
        echo(outcome.describe())
    }

    /** The payload given on the command line, or everything piped in. */
    private fun String?.orStdin(): String {
        val text = this ?: System.`in`.bufferedReader().readText()
        if (text.isBlank()) throw UsageError("no payload given: pass it as an argument or pipe it into stdin")
        return text
    }

    private fun AppendOutcome.describe(): String = when (this) {
        is AppendOutcome.Appended -> "✅ Fact appended. Id: ${factIds.single()}"
        AppendOutcome.AlreadyApplied -> "ℹ️ Append already applied; nothing was added."
    }
}
