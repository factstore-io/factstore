package io.factstore.cli.command

import io.factstore.cli.exception.CliUsageException
import io.factstore.client.model.Fact
import kotlinx.coroutines.flow.Flow
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
    /** A table of fixed column widths, printed as the facts arrive; longer values are cut. */
    Table,

    /** One JSON array, pretty-printed. */
    Json,

    /** One compact JSON object per line. */
    Ndjson,
}

/** Every format writes the facts as they arrive, so a stream of any length can be printed. */
suspend fun Flow<Fact>.print(format: OutputFormat) {
    when (format) {
        OutputFormat.Table -> printTable()
        OutputFormat.Json -> printJsonArray()
        OutputFormat.Ndjson -> collect { println(json.encodeToString(it)) }
    }
}

/** Rejects a limit that is set but not positive; an absent limit means all facts. */
fun requirePositiveLimit(limit: Int?) {
    if (limit != null && limit <= 0) throw CliUsageException("--limit must be positive, but was $limit.")
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

private const val ID_WIDTH = 36
private const val TYPE_WIDTH = 30
private const val SUBJECT_WIDTH = 30
private const val TIME_WIDTH = 20
private const val PAYLOAD_WIDTH = 40

private const val ROW_FORMAT =
    "%-${ID_WIDTH}s  %-${TYPE_WIDTH}s  %-${SUBJECT_WIDTH}s  %-${TIME_WIDTH}s  %s"

/**
 * Writes the facts as a table, a row at a time.
 *
 * The columns have fixed widths rather than ones sized to the facts, which would mean holding
 * the whole stream before printing any of it.
 */
private suspend fun Flow<Fact>.printTable() {
    var first = true
    collect { fact ->
        if (first) {
            println(tableHeader())
            first = false
        }
        println(fact.toTableRow())
    }
    if (first) println("No facts found.")
}

internal fun tableHeader(): String {
    val header = ROW_FORMAT.format("ID", "TYPE", "SUBJECT", "APPENDED AT", "PAYLOAD")
    val separator = "-".repeat(ID_WIDTH + TYPE_WIDTH + SUBJECT_WIDTH + TIME_WIDTH + PAYLOAD_WIDTH + 8)
    return "$header\n$separator"
}

internal fun Fact.toTableRow(): String = ROW_FORMAT.format(
    id,
    type.cutTo(TYPE_WIDTH),
    subject.cutTo(SUBJECT_WIDTH),
    appendedAt.truncatedTo(SECONDS),
    String(payload.data, UTF_8).replace("\n", " ").cutTo(PAYLOAD_WIDTH),
).trimEnd()

/** Cuts a value that is wider than its column, marking the cut. */
private fun String.cutTo(width: Int): String = if (length <= width) this else take(width - 1) + "…"

private val json = Json { prettyPrint = false }
private val prettyJson = Json { prettyPrint = true }


