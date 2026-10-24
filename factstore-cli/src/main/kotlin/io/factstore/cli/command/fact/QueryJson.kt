package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.UsageError
import io.factstore.client.model.FactQueryItem
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
private data class QueryItemJson(
    val subjects: List<String> = emptyList(),
    val types: List<String> = emptyList(),
    val tags: Map<String, String> = emptyMap(),
) {
    fun toFactQueryItem() = FactQueryItem(subjects = subjects, types = types, tags = tags)
}

@Serializable
private data class QueryJson(val items: List<QueryItemJson>)

/** Parses one `--item`; anything but a well-formed query item is a usage error. */
internal fun String.toFactQueryItem(): FactQueryItem =
    try {
        Json.decodeFromString<QueryItemJson>(this).toFactQueryItem()
    } catch (e: SerializationException) {
        throw UsageError("the query item '$this' is not valid: ${e.message}")
    }

/** Reads the items of a `--query-file`; a missing or malformed file is a usage error. */
internal fun File.toFactQueryItems(): List<FactQueryItem> {
    if (!isFile) throw UsageError("the query file '$this' does not exist")
    val text = try {
        readText()
    } catch (e: IOException) {
        throw UsageError("the query file '$this' cannot be read: ${e.message}")
    }
    return try {
        Json.decodeFromString<QueryJson>(text).items.map { it.toFactQueryItem() }
    } catch (e: SerializationException) {
        throw UsageError("the query file '$this' is not valid: ${e.message}")
    }
}
