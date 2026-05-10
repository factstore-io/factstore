package io.factstore.cli.command

import io.factstore.cli.client.FactHttp
import java.time.temporal.ChronoUnit.SECONDS
import kotlin.text.Charsets.UTF_8

fun FactHttp.print() {
    println(
        "[%s] %-15s | %s".format(
            appendedAt?.truncatedTo(SECONDS),
            type,
            String(payload.data, charset = UTF_8)
        )
    )
}

fun printTable(facts: List<FactHttp>) {
    if (facts.isEmpty()) {
        println("No facts found.")
        return
    }

    val idWidth       = 36
    val typeWidth     = facts.maxOf { it.type.length }.coerceAtLeast(4)
    val subjectWidth  = facts.maxOf { it.subject.length }.coerceAtLeast(7)
    val timeWidth     = 20
    val payloadWidth  = 40

    val header = "%-${idWidth}s  %-${typeWidth}s  %-${subjectWidth}s  %-${timeWidth}s  %-${payloadWidth}s"
        .format("ID", "TYPE", "SUBJECT", "APPENDED AT", "PAYLOAD")
    val separator = "-".repeat(idWidth + typeWidth + subjectWidth + timeWidth + payloadWidth + 8)

    println(header)
    println(separator)

    facts.forEach { fact ->
        val payload = String(fact.payload.data, UTF_8)
            .replace("\n", " ")
            .take(payloadWidth)
            .let { if (it.length == payloadWidth) "$it…" else it }

        println(
            "%-${idWidth}s  %-${typeWidth}s  %-${subjectWidth}s  %-${timeWidth}s  %-${payloadWidth}s".format(
                fact.id,
                fact.type,
                fact.subject,
                fact.appendedAt?.truncatedTo(SECONDS) ?: "",
                payload,
            )
        )
    }
}