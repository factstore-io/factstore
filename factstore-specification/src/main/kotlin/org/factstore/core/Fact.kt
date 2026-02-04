package org.factstore.core

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
 * - **Payload** ([payload]) containing the event data and its transport metadata
 * - **Subject association** ([subjectRef]) defining the entity or context
 *   the fact belongs to
 * - **Temporal information** ([appendedAt]) indicating when the fact occurred
 * - **Metadata** ([metadata]) for auxiliary, non-indexed information
 * - **Tags** ([tags]) for classification, filtering, and efficient querying
 *
 * FactStore treats facts as opaque records. It is responsible for storing,
 * indexing, transporting, and replaying facts, but does not interpret the
 * semantic meaning of the payload, schema, or data format.
 *
 * @property id the globally unique identifier of the fact
 * @property type the logical type of the fact
 * @property payload the serialized fact payload
 * @property subjectRef the subject the fact is associated with
 * @property appendedAt the time the fact was appended
 * @property metadata optional metadata associated with the fact
 * @property tags optional tags used for querying and classification
 *
 * @author Domenic Cassisi
 */
data class Fact(
    val id: FactId,
    val type: FactType,
    val payload: FactPayload,
    val subjectRef: SubjectRef,
    val appendedAt: Instant,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<TagKey, TagValue> = emptyMap(),
)

/**
 * Describes the payload of a [Fact] and how it should be interpreted.
 *
 * A [FactPayload] encapsulates the raw payload data together with optional
 * descriptive metadata about its format and schema. This allows clients to
 * understand how to deserialize and process the payload without requiring
 * FactStore to interpret or validate it.
 *
 * The payload data itself is treated as opaque binary data.
 * Interpretation, schema validation, and compatibility guarantees are the
 * responsibility of producers and consumers.
 *
 * This structure intentionally separates payload concerns from the core
 * [Fact] envelope, allowing payload-related metadata to evolve independently
 * without impacting the stability of the fact model.
 *
 * @property data the raw serialized payload data
 * @property format an optional identifier describing the payload data format
 * (for example JSON, Avro, Protobuf, etc.)
 * @property schema an optional reference to the schema used to serialize
 * the payload data
 *
 * @author Domenic Cassisi
 */
data class FactPayload(
    val data: ByteArray,
    val format: PayloadFormat? = null,
    val schema: PayloadSchemaRef? = null
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as FactPayload

        if (!data.contentEquals(other.data)) return false
        if (format != other.format) return false
        if (schema != other.schema) return false

        return true
    }

    override fun hashCode(): Int {
        var result = data.contentHashCode()
        result = 31 * result + format.hashCode()
        result = 31 * result + schema.hashCode()
        return result
    }
}

/**
 * Identifies the data format of a [FactPayload].
 *
 * A [PayloadFormat] describes how the payload bytes are encoded (for example
 * JSON, Avro, or Protobuf). FactStore does not interpret or validate the format;
 * it is provided solely for consumers.
 *
 * @property value the textual representation of the payload format
 */
@JvmInline
value class PayloadFormat(val value: String) {
    init {
        require(value.isNotBlank()) { "Payload format must not be blank" }
    }
}

/**
 * References the schema used to serialize a [FactPayload].
 *
 * A [PayloadSchemaRef] is an opaque identifier that may point to a schema
 * registry entry, a versioned schema name, or any other client-defined
 * schema reference.
 *
 * FactStore does not resolve, validate, or enforce schemas.
 *
 * @property value the schema reference identifier
 */
@JvmInline
value class PayloadSchemaRef(val value: String) {
    init {
        require(value.isNotBlank()) { "Schema ref must not be blank" }
    }
}

/**
 * Identifies the subject a fact belongs to.
 *
 * Subjects define logical groupings of facts and are commonly used to model
 * entities, aggregates, or other consistency boundaries.
 *
 * @property type the subject type
 * @property id the unique identifier of the subject within its type
 *
 * @author Domenic Cassisi
 */
data class SubjectRef(
    val type: String,
    val id: String
)

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
fun String.toPayloadFormat() = PayloadFormat(this)
fun String.toPayloadSchemaRef() = PayloadSchemaRef(this)
