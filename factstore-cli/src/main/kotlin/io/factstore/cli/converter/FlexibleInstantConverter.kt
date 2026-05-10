package io.factstore.cli.converter

import picocli.CommandLine
import picocli.CommandLine.TypeConversionException
import java.time.Instant
import java.time.format.DateTimeParseException

class FlexibleInstantConverter : CommandLine.ITypeConverter<Instant> {

    private val relativePattern = Regex("""^(\d+)([mhd])$""")

    override fun convert(value: String): Instant {
        val match = relativePattern.matchEntire(value.trim())
        if (match != null) {
            val amount = match.groupValues[1].toLong()
            val unit = match.groupValues[2]
            val now = Instant.now()
            return when (unit) {
                "m" -> now.minusSeconds(amount * 60)
                "h" -> now.minusSeconds(amount * 3600)
                "d" -> now.minusSeconds(amount * 86400)
                else -> throw IllegalArgumentException("Unsupported time unit: $unit")
            }
        }

        return try {
            Instant.parse(value)
        } catch (_: DateTimeParseException) {
            throw TypeConversionException(
                "Invalid time value '$value'. Use an ISO instant (e.g. 2024-01-01T00:00:00Z) " +
                        "or a relative duration (e.g. 5m, 2h, 1d)."
            )
        }
    }
}