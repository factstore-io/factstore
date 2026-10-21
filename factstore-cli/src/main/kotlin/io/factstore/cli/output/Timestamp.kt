package io.factstore.cli.output

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

private val TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

/**
 * A timestamp as a table shows it: to the second, in the reader's own time zone.
 *
 * [zone] is a parameter rather than a lookup so the rendering stays pure, and so a test can pin
 * the zone it asserts on.
 */
internal fun Instant.toTableCell(zone: ZoneId = ZoneId.systemDefault()): String =
    TIMESTAMP.format(atZone(zone))
