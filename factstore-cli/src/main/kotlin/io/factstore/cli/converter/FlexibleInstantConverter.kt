package io.factstore.cli.converter

import picocli.CommandLine.ITypeConverter
import picocli.CommandLine.TypeConversionException
import java.time.Duration
import java.time.Instant

class FlexibleInstantConverter : ITypeConverter<Instant> {

    private val relativePattern = Regex("""^(\d+)([mhd])$""")

    override fun convert(value: String): Instant {
        relativePattern.matchEntire(value)?.let { match ->
            val amount = match.groupValues[1].toLong()
            val duration = when (match.groupValues[2]) {
                "m" -> Duration.ofMinutes(amount)
                "h" -> Duration.ofHours(amount)
                "d" -> Duration.ofDays(amount)
                else -> throw TypeConversionException("Unsupported unit: ${match.groupValues[2]}")
            }
            return Instant.now().minus(duration)
        }

        return runCatching { Instant.parse(value) }
            .getOrElse {
                throw TypeConversionException(
                    "Invalid value '$value'. Expected ISO instant (e.g. 2024-01-01T00:00:00Z) or relative duration (e.g. 5m, 2h, 1d)"
                )
            }
    }
}