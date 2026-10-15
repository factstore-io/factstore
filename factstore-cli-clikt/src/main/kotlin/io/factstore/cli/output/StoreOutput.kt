package io.factstore.cli.output

import com.github.ajalt.mordant.table.table
import io.factstore.client.model.StoreInfo
import java.time.ZoneId

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
