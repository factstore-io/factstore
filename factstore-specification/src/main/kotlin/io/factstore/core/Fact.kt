package io.factstore.core

import java.time.Instant
import java.util.*

/**
 * Represents an immutable fact stored in the FactStore.
 *
 * A [Fact] captures something that happened at a specific point in time and is
 * identified by a globally unique [FactId]. Facts are append-only and must not
 * be modified after they have been stored.
 *
 * A fact consists of:
 * - **Identity** ([id]) for uniqueness and idempotency
 * - **Classification** ([type]) describing what kind of fact occurred
 * - **Payload** ([payload]) containing the event data
 * - **Subject association** ([subject]) defining the entity or context
 *   the fact belongs to
 * - **Temporal information** ([appendedAt]) indicating when the fact was ingested
 * - **Metadata** ([metadata]) for auxiliary, non-indexed information
 * - **Tags** ([tags]) for classification, filtering, and efficient querying
 *
 * FactStore treats facts as opaque records. It is responsible for storing,
 * indexing, transporting, and replaying facts, but does not interpret the
 * semantic meaning of the payload, schema, or data format.
 *
 * @property id the globally unique identifier of the fact, assigned by the store
 * @property type the logical type of the fact
 * @property payload the serialized fact payload
 * @property subject the subject the fact is associated with
 * @property appendedAt the server-assigned ingestion time of the fact
 * @property metadata optional metadata associated with the fact
 * @property tags optional tags used for querying and classification
 *
 * @author Domenic Cassisi
 */
data class Fact(
    val id: FactId,
    val type: FactType,
    val payload: FactPayload,
    val subject: Subject,
    val appendedAt: Instant,
    val metadata: Map<MetadataKey, MetadataValue> = emptyMap(),
    val tags: Map<TagKey, TagValue> = emptyMap(),
)

/**
 * Describes the payload of a [Fact].
 *
 * A [FactPayload] carries the raw payload data as opaque binary data.
 * Interpretation, schema validation, and compatibility guarantees are the
 * responsibility of producers and consumers.
 *
 * This structure intentionally separates payload concerns from the core
 * [Fact] envelope, allowing payload handling to evolve independently
 * without impacting the stability of the fact model.
 *
 * @property data the raw serialized payload data
 *
 * @throws IllegalArgumentException if [data] exceeds [MAX_SIZE] bytes
 *
 * @author Domenic Cassisi
 */
data class FactPayload(
    val data: ByteArray,
) {

    companion object {

        /** The maximum size of a payload in bytes: 64 KiB. */
        const val MAX_SIZE = 65_536
    }

    init {
        require(data.size <= MAX_SIZE) {
            "Payload must not exceed $MAX_SIZE bytes, but was ${data.size}."
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as FactPayload

        return data.contentEquals(other.data)
    }

    override fun hashCode(): Int = data.contentHashCode()
}


/**
 * Identifies the logical entity, boundary, or concept a fact belongs to.
 * Subjects are a modeling concept to group related facts together.
 *
 * All facts with the same subject are treated as part of the same history.
 * FactStore treats the subject as a flat, opaque value and does not interpret
 * its structure, so any naming convention that fits the domain may be used —
 * UUIDs, custom identifiers, or hierarchical paths such as `order/12345`.
 *
 * @property value the string representation of the subject. Must conform to
 *         [CLEAN_TEXT_PATTERN] and must not exceed [MAX_LENGTH] characters.
 *
 * @throws IllegalArgumentException if [value] is empty, too long, or contains
 *         characters outside [CLEAN_TEXT_PATTERN]
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class Subject(val value: String) {

    companion object {

        /** The maximum length of a subject. */
        const val MAX_LENGTH = 256
    }

    init {
        requireCleanText(value, "Subject", MAX_LENGTH)
    }
}

/**
 * Globally unique identifier of a fact.
 *
 * FactIds must be unique across the entire FactStore and are used to enforce
 * idempotency and uniqueness guarantees.
 *
 * @property uuid the underlying UUID value
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class FactId(val uuid: UUID) {

    companion object {

        /**
         * Generates a new random [FactId].
         */
        fun generate() = FactId(UUID.randomUUID())

    }
}

/**
 * Identifies the logical type of a [Fact].
 *
 * A [FactType] represents the semantic classification of a fact, such as
 * `"OrderCreated"` or `"PaymentAuthorized"`. Fact types are used for
 * categorization, querying, and downstream processing, but FactStore does
 * not impose any domain-specific semantics or schema constraints on them.
 *
 * The value is treated as an opaque identifier. Naming conventions and
 * lifecycle management of fact types are intentionally left to clients;
 * both `ORDER_PLACED` and `com.acme.OrderPlaced` conform.
 *
 * @property value the textual representation of the fact type. Must conform to
 *         [CLEAN_TEXT_PATTERN] and must not exceed [MAX_LENGTH] characters.
 *
 * @throws IllegalArgumentException if [value] is empty, too long, or contains
 *         characters outside [CLEAN_TEXT_PATTERN]
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class FactType(val value: String) {

    companion object {

        /** The maximum length of a fact type. */
        const val MAX_LENGTH = 256
    }

    init {
        requireCleanText(value, "Fact type", MAX_LENGTH)
    }
}

/**
 * Identifies a tag key used to classify or annotate facts.
 *
 * A [TagKey] represents the name of a tag, such as `"region"`, `"tenant"`,
 * or `"archived"`. Tag keys are used in combination with [TagValue]s to
 * support flexible querying and secondary indexing.
 *
 * Tag keys name a dimension rather than carry data, so they are short by
 * nature.
 *
 * @property value the textual representation of the tag key. Must conform to
 *         [CLEAN_TEXT_PATTERN] and must not exceed [MAX_LENGTH] characters.
 *
 * @throws IllegalArgumentException if [value] is empty, too long, or contains
 *         characters outside [CLEAN_TEXT_PATTERN]
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class TagKey(val value: String) {

    companion object {

        /** The maximum length of a tag key. */
        const val MAX_LENGTH = 128
    }

    init {
        requireCleanText(value, "Tag key", MAX_LENGTH)
    }
}

/**
 * Represents the value associated with a [TagKey].
 *
 * A [TagValue] may either carry a meaningful value (for example `"eu"` or
 * `"v2"`) or be empty to indicate presence-only semantics. Empty values are
 * commonly used to express boolean-like or classificatory tags, such as
 * `"archived"`.
 *
 * FactStore does not impose any interpretation on tag values; their meaning
 * is entirely defined by the client. It does, however, use them as index keys,
 * so they are restricted to [CLEAN_TEXT_PATTERN]. Values that need a wider
 * character set belong in the fact payload, which is stored as opaque bytes and
 * is never validated or indexed.
 *
 * @property value the textual representation of the tag value. Must either be
 *         empty or conform to [CLEAN_TEXT_PATTERN], and must not exceed
 *         [MAX_LENGTH] characters.
 *
 * @throws IllegalArgumentException if [value] is too long, or is non-empty and
 *         contains characters outside [CLEAN_TEXT_PATTERN]
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class TagValue(val value: String) {

    companion object {

        /** The maximum length of a tag value. */
        const val MAX_LENGTH = 256
    }

    init {
        requireCleanText(value, "Tag value", MAX_LENGTH, allowEmpty = true)
    }
}

/**
 * Identifies a metadata entry attached to a [Fact].
 *
 * A [MetadataKey] names a piece of auxiliary information about a fact — a
 * correlation id, a causation id, the producing service — as opposed to the
 * fact's own data, which belongs in the payload.
 *
 * Metadata is not indexed and cannot be queried. Use [TagKey] and [TagValue]
 * for anything facts need to be selected by.
 *
 * @property value the textual representation of the metadata key. Must conform
 *         to [CLEAN_TEXT_PATTERN] and must not exceed [MAX_LENGTH] characters.
 *
 * @throws IllegalArgumentException if [value] is empty, too long, or contains
 *         characters outside [CLEAN_TEXT_PATTERN]
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class MetadataKey(val value: String) {

    companion object {

        /** The maximum length of a metadata key. */
        const val MAX_LENGTH = 128
    }

    init {
        requireCleanText(value, "Metadata key", MAX_LENGTH)
    }
}

/**
 * Represents the value associated with a [MetadataKey].
 *
 * As with [TagValue], an empty value is permitted and carries presence-only
 * meaning. FactStore does not interpret metadata values.
 *
 * @property value the textual representation of the metadata value. Must either
 *         be empty or conform to [CLEAN_TEXT_PATTERN], and must not exceed
 *         [MAX_LENGTH] characters.
 *
 * @throws IllegalArgumentException if [value] is too long, or is non-empty and
 *         contains characters outside [CLEAN_TEXT_PATTERN]
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class MetadataValue(val value: String) {

    companion object {

        /** The maximum length of a metadata value. */
        const val MAX_LENGTH = 256
    }

    init {
        requireCleanText(value, "Metadata value", MAX_LENGTH, allowEmpty = true)
    }
}

/**
 * Converts a [UUID] to a [FactId].
 */
fun UUID.toFactId() = FactId(this)
fun String.toSubject() = Subject(this)
fun String.toFactType() = FactType(this)
fun String.toTagKey() = TagKey(this)
fun String.toTagValue() = TagValue(this)
fun String.toMetadataKey() = MetadataKey(this)
fun String.toMetadataValue() = MetadataValue(this)
fun String.toFactPayload() = FactPayload(this.toByteArray())
