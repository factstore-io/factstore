package io.factstore.cli.command

import io.factstore.client.model.Fact
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.json.Json
import java.time.temporal.ChronoUnit.SECONDS
import kotlin.text.Charsets.UTF_8

fun Fact.printSingle(outputFormat: OutputFormat) {
    when (outputFormat) {
        OutputFormat.Table -> printTable()
        OutputFormat.Json -> println(prettyJson.encodeToString(this))
        OutputFormat.Ndjson -> println(json.encodeToString(this))
    }
}

fun Fact.printTable() {
    println(
        "[%s] %-15s | %s".format(
            appendedAt.truncatedTo(SECONDS),
            type,
            String(payload.data, charset = UTF_8)
        )
    )
}

enum class OutputFormat {
    /** A table, sized to its contents. */
    Table,

    /** One JSON array, pretty-printed. */
    Json,

    /** One compact JSON object per line. */
    Ndjson,
}

suspend fun Flow<Fact>.print(format: OutputFormat) {
    when (format) {
        OutputFormat.Table -> toList().printTable()
        OutputFormat.Json -> printJsonArray()
        OutputFormat.Ndjson -> collect { println(json.encodeToString(it)) }
    }
}

/**
 * Writes the facts as one pretty-printed JSON array, a fact at a time.
 */
private suspend fun Flow<Fact>.printJsonArray() {
    var first = true
    collect { fact ->
        println(if (first) "[" else ",")
        print(prettyJson.encodeToString(fact).prependIndent("  "))
        first = false
    }
    if (first) println("[]") else println("\n]")
}



private fun List<Fact>.printTable() {
    if (this.isEmpty()) {
        println("No facts found.")
        return
    }

    val idWidth       = 36
    val typeWidth     = this.maxOf { it.type.length }.coerceAtLeast(4)
    val subjectWidth  = this.maxOf { it.subject.length }.coerceAtLeast(7)
    val timeWidth     = 20
    val payloadWidth  = 40

    val header = "%-${idWidth}s  %-${typeWidth}s  %-${subjectWidth}s  %-${timeWidth}s  %-${payloadWidth}s"
        .format("ID", "TYPE", "SUBJECT", "APPENDED AT", "PAYLOAD")
    val separator = "-".repeat(idWidth + typeWidth + subjectWidth + timeWidth + payloadWidth + 8)

    println(header)
    println(separator)

    this.forEach { fact ->
        val payload = String(fact.payload.data, UTF_8)
            .replace("\n", " ")
            .take(payloadWidth)
            .let { if (it.length == payloadWidth) "$it…" else it }

        println(
            "%-${idWidth}s  %-${typeWidth}s  %-${subjectWidth}s  %-${timeWidth}s  %-${payloadWidth}s".format(
                fact.id,
                fact.type,
                fact.subject,
                fact.appendedAt.truncatedTo(SECONDS) ?: "",
                payload,
            )
        )
    }
}

private val json = Json { prettyPrint = false }
private val prettyJson = Json { prettyPrint = true }


