package io.factstore.cli.converter

import io.factstore.cli.exception.CliUsageException
import picocli.CommandLine.ITypeConverter
import picocli.CommandLine.TypeConversionException

class TagConverter : ITypeConverter<Pair<String, String>> {
    override fun convert(value: String): Pair<String, String> {
        val parts = value.split("=", limit = 2)
        if (parts.size != 2 || parts[0].isBlank()) {
            throw TypeConversionException(
                "Tag must be in key=value format, got: '$value'"
            )
        }
        return parts[0].trim() to parts[1]
    }
}

/**
 * The tags a fact must carry, all of them: a key may therefore be given only once, as the
 * server requires of its own tag parameters.
 */
internal fun List<Pair<String, String>>.toTagFilter(): Map<String, String> = buildMap {
    for ((key, value) in this@toTagFilter) {
        if (key in this) {
            throw CliUsageException("Tag '$key' is given more than once. To match any of several values, use 'fact query'.")
        }
        put(key, value)
    }
}
