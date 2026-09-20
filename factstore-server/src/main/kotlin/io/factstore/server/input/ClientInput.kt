package io.factstore.server.input

import io.factstore.core.FactId
import io.factstore.core.FactType
import io.factstore.core.Limit
import io.factstore.core.MetadataKey
import io.factstore.core.MetadataValue
import io.factstore.core.ReadDirection
import io.factstore.core.StoreName
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagValue
import io.factstore.core.toFactType
import io.factstore.core.toMetadataKey
import io.factstore.core.toMetadataValue
import io.factstore.core.toStoreName
import io.factstore.core.toSubject
import io.factstore.core.toTagKey
import io.factstore.core.toTagValue
import java.time.Instant
import java.util.UUID

/*
 * Conversions from untrusted client input to the specification's value types.
 */

internal fun String.asStoreName(): StoreName = trim().toStoreName()

internal fun String.asSubject(): Subject = trim().toSubject()

internal fun String.asFactType(): FactType = trim().toFactType()

internal fun String.asTagKey(): TagKey = trim().toTagKey()

internal fun String.asTagValue(): TagValue = trim().toTagValue()

internal fun String.asMetadataKey(): MetadataKey = trim().toMetadataKey()

internal fun String.asMetadataValue(): MetadataValue = trim().toMetadataValue()

internal fun Map<String, String>.asTags(): Map<TagKey, TagValue> =
    entries.associate { (key, value) -> key.asTagKey() to value.asTagValue() }

internal fun Map<String, String>.asMetadata(): Map<MetadataKey, MetadataValue> =
    entries.associate { (key, value) -> key.asMetadataKey() to value.asMetadataValue() }

internal fun String.asUuid(): UUID =
    try {
        UUID.fromString(trim())
    } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("'$this' is not a valid UUID.", e)
    }

internal fun String.asFactId(): FactId = FactId(asUuid())

internal fun String?.asInstant(): Instant? = this?.let { Instant.parse(it.trim()) }

/** Absent means no limit; anything but a positive integer is rejected. */
internal fun String?.asLimit(): Limit {
    if (this == null) return Limit.None
    val value = trim().toIntOrNull()
    requireNotNull(value) { "Limit must be a positive integer, but was '$this'." }
    return Limit.of(value)
}

/**
 * An absent direction reads forward, the natural order of an append-only log.
 */
internal fun String?.asReadDirection(): ReadDirection = when (this?.trim()?.lowercase()) {
    null, "forward" -> ReadDirection.Forward
    "backward" -> ReadDirection.Backward
    else -> throw IllegalArgumentException("Direction must be 'forward' or 'backward', but was '$this'.")
}

/** Tags given as `key=value` strings, such as query parameters. */
internal fun List<String>.asTagFilter(): Map<TagKey, TagValue> = associate { tag ->
    val parts = tag.split("=", limit = 2)
    require(parts.size == 2) { "A tag must have the form key=value, but was '$tag'." }
    parts[0].asTagKey() to parts[1].asTagValue()
}
