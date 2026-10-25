package io.factstore.cli.output

import com.github.ajalt.mordant.table.table
import com.github.ajalt.mordant.terminal.Terminal
import io.factstore.client.model.StoreInfo
import java.time.ZoneId

/**
 * Writes the stores.
 *
 * Unlike a store's history, a list of stores is short and bounded, so it can be held long enough
 * to size a table's columns to it. The JSON shapes go to [out], the way machine-readable output
 * does everywhere: what the reader asked for, and not a character more.
 */
internal fun List<StoreInfo>.writeTo(
    terminal: Terminal,
    format: OutputFormat,
    out: Appendable = bufferedStdout(),
) {
    try {
        when (format) {
            OutputFormat.Table ->
                if (isEmpty()) terminal.println("No stores found.") else terminal.println(toStoreTable())
            OutputFormat.Json -> out.appendLine(prettyJson.encodeToString(map { it.toJson() }))
            OutputFormat.Ndjson -> forEach { out.appendLine(compactJson.encodeToString(it.toJson())) }
        }
    } finally {
        out.flushIfPossible()
    }
}

/**
 * The stores as a table, column widths and alignment left to the renderer.
 *
 */
internal fun List<StoreInfo>.toStoreTable(zone: ZoneId = ZoneId.systemDefault()) = table {
    header { row("NAME", "CREATED AT", "STORE ID") }
    body {
        for (store in this@toStoreTable) {
            row(
                store.name,
                store.createdAt.toTableCell(zone),
                store.id,
            )
        }
    }
}
