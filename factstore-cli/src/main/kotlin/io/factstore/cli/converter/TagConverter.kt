package io.factstore.cli.converter

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
