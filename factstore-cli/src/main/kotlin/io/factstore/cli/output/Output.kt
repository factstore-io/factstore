package io.factstore.cli.output

import kotlinx.serialization.json.Json
import java.io.BufferedWriter
import java.io.Flushable
import java.io.OutputStreamWriter

/*
 * What every shape of output shares: how it is encoded, and where it is written.
 */

private const val BUFFER_BYTES = 64 * 1024

/** One object per line, for a reader that is a program. */
internal val compactJson = Json { prettyPrint = false }

/** Indented, for output a person may also read. */
internal val prettyJson = Json { prettyPrint = true }

/** Standard output, buffered: a fact at a time would be a write to the connection at a time. */
internal fun bufferedStdout(): Appendable =
    BufferedWriter(OutputStreamWriter(System.out, Charsets.UTF_8), BUFFER_BYTES)

internal fun Appendable.flushIfPossible() = (this as? Flushable)?.flush()
