package io.factstore.cli.output

import com.github.ajalt.mordant.terminal.Terminal
import io.factstore.client.model.Fact
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf

private const val PAYLOAD_PREVIEW = 40

/** A run of whitespace, compiled once rather than for every row of a stream. */
private val WHITESPACE = Regex("\\s+")

/**
 * Writes the facts of a bounded read.
 *
 * Every shape is written as the facts arrive, so a long history can be piped onwards -- or read
 * -- while it is still being fetched, and a store larger than memory is no harder to read than a
 * small one.
 *
 * Nothing goes through the terminal. Machine-readable output has to stay exactly what the reader
 * asked for, and a terminal renderer reads every character on the way past -- about a second of
 * it for a stream of 100k facts. The terminal is asked for its width and nothing else.
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
            OutputFormat.Table -> writeTable(FactColumns(terminal.size.width), out)
            OutputFormat.Json -> writeJsonArray(out)
            OutputFormat.Ndjson -> collect { out.appendLine(compactJson.encodeToString(it.toJson())) }
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
 * Writes the facts of a subscription, which has no last fact: each one is written and flushed as
 * it arrives -- a table as a line each, the way `tail -f` writes lines -- so a follower sees a
 * fact the moment it is appended.
 *
 * A subscription never ends, so there is no point at which a JSON array could be closed; that is
 * why `fact subscribe` offers [OutputFormat.Table] and [OutputFormat.Ndjson] and nothing else.
 */
internal suspend fun Flow<Fact>.followTo(
    terminal: Terminal,
    format: OutputFormat,
    out: Appendable = bufferedStdout(),
): Unit = when (format) {
    OutputFormat.Table -> collect { terminal.println(it.toLine()) }
    OutputFormat.Ndjson -> collect { out.appendLine(compactJson.encodeToString(it.toJson())).flushIfPossible() }
    OutputFormat.Json -> error("a followed stream cannot be one JSON array")
}

/** One fact on one line, for a stream a reader is watching go by. */
internal fun Fact.toLine(): String =
    "[${appendedAt.toTableCell()}] ${type.padEnd(15)} | ${payloadText().cutTo(PAYLOAD_PREVIEW)}"

/** A header, a rule, and then a row per fact as it arrives. */
private suspend fun Flow<Fact>.writeTable(columns: FactColumns, out: Appendable) {
    out.appendLine(columns.header()).appendLine(columns.rule())
    collect { out.appendLine(columns.row(it)) }
}

/**
 * The widths of a fact table's columns, settled before the first row is written.
 *
 * Columns sized to the facts would read better, but their widths are only known once the last
 * fact is in -- which means holding a whole history in memory before printing any of it. These
 * are worked out from the terminal's width instead, so a row can be written the moment it
 * arrives.
 *
 * An id is never cut, being what the next command is given, and a timestamp has a width of its
 * own. What is left of the line is shared out between the other three, and the payload takes the
 * remainder as the column that can best use it.
 */
private class FactColumns(terminalWidth: Int) {

    private val free = (terminalWidth - ID_WIDTH - TIME_WIDTH - GAP.length * 4).coerceAtLeast(MIN * 3)

    private val type = (free * 3 / 10).coerceIn(MIN, TYPE_MAX)
    private val subject = (free * 3 / 10).coerceIn(MIN, SUBJECT_MAX)
    private val payload = free - type - subject

    fun header(): String = line("ID", "TYPE", "SUBJECT", "APPENDED AT", "PAYLOAD")

    fun rule(): String = "-".repeat(ID_WIDTH + type + subject + TIME_WIDTH + payload + GAP.length * 4)

    fun row(fact: Fact): String = line(
        fact.id,
        fact.type,
        fact.subject,
        fact.appendedAt.toTableCell(),
        fact.payloadText(),
    )

    private fun line(id: String, type: String, subject: String, time: String, payload: String): String =
        listOf(
            id.fit(ID_WIDTH),
            type.fit(this.type),
            subject.fit(this.subject),
            time.fit(TIME_WIDTH),
            payload.fit(this.payload),
        ).joinToString(GAP).trimEnd()

    /** A cell cut down to its column, then padded out to it. */
    private fun String.fit(width: Int): String = cutTo(width).padEnd(width)

    private companion object {
        /** A fact id is a UUID, which is always this wide. */
        const val ID_WIDTH = 36

        /** `yyyy-MM-dd HH:mm:ss`, as [toTableCell] writes it. */
        const val TIME_WIDTH = 19

        const val GAP = "  "

        /**
         * The narrowest a shared column is allowed to get.
         *
         * At 5 the whole row fits an 80-column terminal, which is as narrow as a table with a
         * full fact id in it can be; below that the terminal wraps the row, and `-o ndjson`
         * is the better answer anyway.
         */
        const val MIN = 5
        const val TYPE_MAX = 30
        const val SUBJECT_MAX = 40
    }
}

/**
 * The payload as one line of text.
 *
 * A payload is opaque bytes that may hold anything, including newlines and more of them than a
 * terminal can show, and a row that wraps a whole document is unreadable.
 */
private fun Fact.payloadText(): String =
    String(payload.data, Charsets.UTF_8).replace(WHITESPACE, " ").trim()

/** Cut to [width], with the cut marked, unless it already fits. */
private fun String.cutTo(width: Int): String =
    if (length <= width) this else take(width - 1) + "…"

/** The facts as one JSON array, written as they arrive. */
private suspend fun Flow<Fact>.writeJsonArray(out: Appendable) {
    var first = true
    collect { fact ->
        out.appendLine(if (first) "[" else ",")
        out.append(prettyJson.encodeToString(fact.toJson()).prependIndent("  "))
        first = false
    }
    if (first) out.appendLine("[]") else out.appendLine().appendLine("]")
}
