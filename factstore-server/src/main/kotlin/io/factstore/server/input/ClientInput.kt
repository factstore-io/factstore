package io.factstore.server.input

import io.factstore.core.FactType
import io.factstore.core.MetadataKey
import io.factstore.core.MetadataValue
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
