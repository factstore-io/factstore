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
    val metadata: Map<String, String> = emptyMap(),
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
 * @author Domenic Cassisi
 */
data class FactPayload(
    val data: ByteArray,
) {

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
 * FactStore treats the subject as a flat, opaque string, allowing you to use
 * any naming convention that fits your domain (e.g., UUIDs, custom identifiers,
 * or hierarchical paths).
 *
 * @property value The string representation of the subject.
 *                 Must not be blank and must not contain leading or trailing whitespace.
 * @author Domenic Cassisi
 */
@JvmInline
value class Subject(val value: String) {
    init {
        require(value.isNotBlank()) { "Subject must not be blank" }
        require(value == value.trim()) { "Subject must not contain leading or trailing whitespaces" }
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
 * The value is treated as an opaque, non-blank identifier. Naming conventions
 * and lifecycle management of fact types are intentionally left to clients.
 *
 * @property value the textual representation of the fact type
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class FactType(val value: String) {
    init {
        require(value.isNotBlank()) { "Type must not be blank" }
    }
}

/**
 * Identifies a tag key used to classify or annotate facts.
 *
 * A [TagKey] represents the name of a tag, such as `"region"`, `"tenant"`,
 * or `"archived"`. Tag keys are used in combination with [TagValue]s to
 * support flexible querying and secondary indexing.
 *
 * Tag keys are required to be non-blank. No additional constraints or
 * naming conventions are enforced by FactStore.
 *
 * @property value the textual representation of the tag key
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class TagKey(val value: String) {
    init {
        require(value.isNotBlank()) { "TagKey must not be blank" }
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
 * is entirely defined by the client.
 *
 * @property value the textual representation of the tag value, which may be empty
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class TagValue(val value: String)

/**
 * Converts a [UUID] to a [FactId].
 */
fun UUID.toFactId() = FactId(this)
fun String.toFactType() = FactType(this)
fun String.toTagKey() = TagKey(this)
fun String.toTagValue() = TagValue(this)
fun String.toFactPayload() = FactPayload(this.toByteArray())
