package io.factstore.cli.command

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.factstore.cli.client.FactHttp
import java.time.temporal.ChronoUnit.SECONDS
import kotlin.text.Charsets.UTF_8

fun FactHttp.printSingle(outputFormat: OutputFormat) {
    when (outputFormat) {
        OutputFormat.Table -> printTable()
        OutputFormat.Json -> printJson()
    }
}

fun FactHttp.printTable() {
    println(
        "[%s] %-15s | %s".format(
            appendedAt?.truncatedTo(SECONDS),
            type,
            String(payload.data, charset = UTF_8)
        )
    )
}

enum class OutputFormat {
    Table,
    Json
}

fun List<FactHttp>.print(format: OutputFormat) {
    when (format) {
        OutputFormat.Table -> printTable()
        OutputFormat.Json -> printPrettyJson()
    }
}

fun List<FactHttp>.printTable() {
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
                fact.appendedAt?.truncatedTo(SECONDS) ?: "",
                payload,
            )
        )
    }
}

val jsonPrinter: ObjectMapper = ObjectMapper()
    .registerModule(JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)

val prettyJsonPrinter: ObjectWriter =
    jsonPrinter.writerWithDefaultPrettyPrinter()


fun List<FactHttp>.printPrettyJson() {
    println(prettyJsonPrinter.writeValueAsString(this))
}

fun FactHttp.printJson() {
    println(jsonPrinter.writeValueAsString(this))
}
