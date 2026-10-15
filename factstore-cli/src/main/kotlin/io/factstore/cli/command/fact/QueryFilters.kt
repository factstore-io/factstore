package io.factstore.cli.command.fact

import io.factstore.cli.exception.CliUsageException
import io.factstore.client.model.FactFilter
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import java.io.File
import java.io.IOException

/*
 * The JSON a query is given in, on the command line or in a file.
 *
 * It is decoded strictly: a misspelled property would otherwise be dropped, and the query would
 * silently match more than was asked for.
 */

@Serializable
private data class FilterJson(
    val subjects: List<String> = emptyList(),
    val types: List<String> = emptyList(),
    val tags: Map<String, String> = emptyMap(),
) {
    fun toFactFilter() = FactFilter(subjects = subjects, types = types, tags = tags)
}

@Serializable
private data class QueryJson(val filters: List<FilterJson>)

/** Parses one `--filter`; anything but a well-formed filter is a usage error. */
internal fun parseFilter(text: String): FactFilter =
    try {
        Json.decodeFromString<FilterJson>(text).toFactFilter()
    } catch (e: SerializationException) {
        throw CliUsageException("The filter '$text' is not valid: ${e.message}")
    }

/** Reads the filters of a `--query-file`; a missing or malformed file is a usage error. */
internal fun readQueryFile(file: File): List<FactFilter> {
    if (!file.isFile) throw CliUsageException("The query file '$file' does not exist.")
    val text = try {
        file.readText()
    } catch (e: IOException) {
        throw CliUsageException("The query file '$file' cannot be read: ${e.message}")
    }
    return try {
        Json.decodeFromString<QueryJson>(text).filters.map { it.toFactFilter() }
    } catch (e: SerializationException) {
        throw CliUsageException("The query file '$file' is not valid: ${e.message}")
    }
}
