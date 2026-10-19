package io.factstore.cli.output

import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.terminal.Terminal
import io.factstore.client.model.Fact
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.json.Json
import java.io.BufferedWriter
import java.io.Flushable
import java.io.OutputStreamWriter

private const val PAYLOAD_PREVIEW = 40
private const val BUFFER_BYTES = 64 * 1024

private val compactJson = Json { prettyPrint = false }
private val prettyJson = Json { prettyPrint = true }

/**
 * Writes the facts of a bounded read.
 *
 * A table goes through the terminal, which sizes and styles it for whoever is watching. The JSON
 * shapes go straight to [out] instead, by default a buffered standard output: machine-readable
 * output has to stay exactly what the reader asked for, and a terminal renderer reads every
 * character on the way past -- about a second of it for a stream of 100k facts.
 *
 * Both JSON shapes are written as the facts arrive, so a long history can be piped onwards while
 * it is still being read. A table cannot: its columns are only as wide as its widest row, which
 * is known once the last fact is in.
 *
 * A table is rendered to the width Mordant reports, which is 79 columns when it cannot see a
 * terminal -- narrow enough to cut a fact's id. That is the reason `ndjson` exists for pipelines,
 * and `COLUMNS` widens a table on the way to a pager.
 */
internal suspend fun Flow<Fact>.writeTo(
    terminal: Terminal,
    format: OutputFormat,
    out: Appendable = bufferedStdout(),
) {
    // Flushed even when the read fails part-way: whatever arrived before the failure has been
    // written already, and dropping a bufferful of it would leave the reader an error and a
    // silently truncated file.
    try {
        when (format) {
            OutputFormat.Table -> terminal.println(toList().toFactTable())
            OutputFormat.Json -> writeJsonArray(out)
            OutputFormat.Ndjson -> collect { out.appendLine(compactJson.encodeToString(it)) }
        }
    } finally {
        out.flushIfPossible()
    }
}

internal suspend fun Fact.writeTo(
    terminal: Terminal,
    format: OutputFormat,
    out: Appendable = bufferedStdout(),
): Unit = flowOf(this).writeTo(terminal, format, out)

/**
 * Writes the facts of a subscription, which has no last fact: every shape is written one fact at
 * a time, a table included -- as a line each, the way `tail -f` writes lines -- and flushed, so a
 * follower sees a fact the moment it arrives.
 */
internal suspend fun Flow<Fact>.followTo(
    terminal: Terminal,
    format: OutputFormat,
    out: Appendable = bufferedStdout(),
): Unit = when (format) {
    OutputFormat.Table -> collect { terminal.println(it.toLine()) }
    OutputFormat.Json -> collect { out.appendLine(prettyJson.encodeToString(it)).flushIfPossible() }
    OutputFormat.Ndjson -> collect { out.appendLine(compactJson.encodeToString(it)).flushIfPossible() }
}

internal fun List<Fact>.toFactTable() = table {
    header { row("ID", "TYPE", "SUBJECT", "APPENDED AT", "PAYLOAD") }
    body {
        for (fact in this@toFactTable) {
            row(fact.id, fact.type, fact.subject, fact.appendedAt.toTableCell(), fact.payloadPreview())
        }
    }
}

/** One fact on one line, for a stream a reader is watching go by. */
internal fun Fact.toLine(): String =
    "[${appendedAt.toTableCell()}] ${type.padEnd(15)} | ${payloadPreview()}"

/**
 * The payload as text, cut to one short line.
 *
 * A payload is opaque bytes that may hold anything, including newlines and more of them than a
 * terminal can show, and a table that wraps a whole document is unreadable.
 */
private fun Fact.payloadPreview(limit: Int = PAYLOAD_PREVIEW): String {
    val text = String(payload.data, Charsets.UTF_8).replace(Regex("\\s+"), " ").trim()
    return if (text.length <= limit) text else text.take(limit) + "…"
}

/** The facts as one JSON array, written as they arrive. */
private suspend fun Flow<Fact>.writeJsonArray(out: Appendable) {
    var first = true
    collect { fact ->
        out.appendLine(if (first) "[" else ",")
        out.append(prettyJson.encodeToString(fact).prependIndent("  "))
        first = false
    }
    if (first) out.appendLine("[]") else out.appendLine().appendLine("]")
}

/** Standard output, buffered: a fact at a time would be a write to the connection at a time. */
private fun bufferedStdout(): Appendable =
    BufferedWriter(OutputStreamWriter(System.out, Charsets.UTF_8), BUFFER_BYTES)

private fun Appendable.flushIfPossible() = (this as? Flushable)?.flush()
