package org.factstore.core

import java.time.Instant
import java.util.*

/**
 * Represents an immutable fact stored in the FactStore.
 *
 * A fact captures something that happened at a specific point in time and is
 * identified by a globally unique [FactId]. Facts are append-only and must not
 * be modified after they have been stored.
 *
 * The payload is treated as opaque binary data by the FactStore. Interpretation
 * and schema validation are the responsibility of the client.
 *
 * @property id the globally unique identifier of the fact
 * @property type the logical type of the fact
 * @property payload the serialized fact payload
 * @property subjectRef the subject the fact is associated with
 * @property createdAt the time the fact was created
 * @property metadata optional metadata associated with the fact
 * @property tags optional tags used for querying and classification
 *
 * @author Domenic Cassisi
 */
data class Fact(
    val id: FactId,
    val type: FactType,
    val payload: ByteArray,
    val subjectRef: SubjectRef,
    val createdAt: Instant,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<TagKey, TagValue> = emptyMap(),
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Fact

        if (id != other.id) return false
        if (type != other.type) return false
        if (!payload.contentEquals(other.payload)) return false
        if (subjectRef != other.subjectRef) return false
        if (createdAt != other.createdAt) return false
        if (metadata != other.metadata) return false
        if (tags != other.tags) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + subjectRef.hashCode()
        result = 31 * result + createdAt.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + tags.hashCode()
        return result
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
