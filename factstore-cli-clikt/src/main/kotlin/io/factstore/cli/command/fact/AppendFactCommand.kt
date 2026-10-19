package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.optional
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import io.factstore.cli.command.StoreScopedCommand
import io.factstore.cli.command.tagOption
import io.factstore.cli.command.usageError
import io.factstore.cli.command.uuid
import io.factstore.cli.connection.FactStoreClients
import io.factstore.client.model.AppendOutcome
import io.factstore.client.model.FactInput
import io.factstore.client.model.FactPayload
import jakarta.enterprise.context.Dependent

/** `factstore fact append <payload>` */
@Dependent
class AppendFactCommand(clients: FactStoreClients) : StoreScopedCommand(name = "append", clients = clients) {

    override fun help(context: Context) = "Append a fact to a store"

    private val type by option("--type", metavar = "<type>", help = "What happened, e.g. ORDER_PLACED").required()
    private val subject by option("--subject", metavar = "<subject>", help = "The entity it happened to, e.g. order/12345").required()
    private val tags by tagOption(
        help = "A tag to carry, as key=value, repeatable",
        duplicateHint = "a fact carries one value per key",
    )
    private val payload by argument("payload", help = "The fact's payload; read from stdin when absent").optional()

    /**
     * `--idempotency-key`: what makes an append safe to repeat.
     *
     * Without one the server makes a key up, which is as good as none: a retry after an
     * ambiguous timeout appends the fact a second time. The same key twice appends it once.
     */
    private val idempotencyKey by option(
        "--idempotency-key",
        metavar = "<uuid>",
        help = "Append at most once for this key, so a retry cannot duplicate the fact",
    ).convert { uuid("an idempotency key", it) }

    override suspend fun run() {
        val outcome = client.facts.append(
            storeName = storeName,
            facts = listOf(
                FactInput(
                    type = type,
                    subject = subject,
                    payload = FactPayload(payload.orStdin().toByteArray()),
                    tags = tags.toMap(),
                )
            ),
            idempotencyKey = idempotencyKey,
        )
        echo(outcome.describe())
    }

    /**
     * The payload given on the command line, or everything piped in.
     *
     * Stdin is only read when something is feeding it. Reading it from an interactive terminal
     * would leave the command sitting there with no prompt, looking hung, until the user found
     * Ctrl-D -- so with neither an argument nor a pipe, this says what was missing instead.
     */
    private fun String?.orStdin(): String {
        val text = this ?: if (interactive()) "" else System.`in`.bufferedReader().readText()
        if (text.isBlank()) throw usageError("no payload given: pass it as an argument or pipe it into stdin")
        return text
    }

    /** Whether the user is sitting at a terminal rather than piping this command's input. */
    private fun interactive(): Boolean = System.console()?.isTerminal() == true

    private fun AppendOutcome.describe(): String = when (this) {
        is AppendOutcome.Appended -> "✅ Fact appended. Id: ${factIds.single()}"
        AppendOutcome.AlreadyApplied -> "ℹ️ Append already applied; nothing was added."
    }
}
