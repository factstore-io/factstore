package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.parameters.arguments.argument
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

/** The payload argument that stands for stdin. */
private const val STDIN = "-"

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
    private val payload by argument("payload", help = "The fact's payload, or - to read it from stdin")

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
                    payload = FactPayload(payloadText().toByteArray()),
                    tags = tags.toMap(),
                )
            ),
            idempotencyKey = idempotencyKey,
        )
        echo(outcome.describe())
    }

    /**
     * The payload given on the command line, or everything on stdin for `-`.
     *
     * Stdin is read only when asked for, as `cat -` and `kubectl apply -f -` do. Reading it
     * whenever the argument is missing would need to know whether stdin is a terminal, or the
     * command would sit waiting on the keyboard with no prompt -- and nothing answers that
     * reliably: `System.console()` asks about stdout as well, and Mordant answers yes whenever
     * none of its native backends could load, which is every run without native access.
     */
    private fun payloadText(): String {
        val text = if (payload == STDIN) System.`in`.bufferedReader().readText() else payload
        if (text.isBlank()) throw usageError("the payload is empty")
        return text
    }

    private fun AppendOutcome.describe(): String = when (this) {
        is AppendOutcome.Appended -> "✅ Fact appended. Id: ${factIds.single()}"
        AppendOutcome.AlreadyApplied -> "ℹ️ Append already applied; nothing was added."
    }
}
