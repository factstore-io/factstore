package io.factstore.core

import java.time.Instant

/**
 * The client-supplied shape of a fact to be appended.
 *
 * A [FactInput] carries only the information a producer owns: the logical
 * [type], the [subject], the [payload], and optional [metadata] and [tags].
 *
 * The fact's identity ([Fact.id]) and ingestion timestamp ([Fact.appendedAt])
 * are **assigned by the FactStore** when the fact is persisted — clients cannot
 * set them. This keeps identity and time authoritative and prevents clients from
 * backdating facts or forging identifiers. The stored, server-assigned
 * representation is [Fact].
 *
 * @property type the logical type of the fact
 * @property subject the subject the fact is associated with
 * @property payload the serialized fact payload
 * @property metadata optional metadata associated with the fact
 * @property tags optional tags used for querying and classification
 *
 * @author Domenic Cassisi
 */
data class FactInput(
    val type: FactType,
    val subject: Subject,
    val payload: FactPayload,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<TagKey, TagValue> = emptyMap(),
)

/**
 * Materializes this [FactInput] into a stored [Fact] using the server-assigned
 * [id] and ingestion [appendedAt] timestamp.
 */
fun FactInput.toFact(id: FactId, appendedAt: Instant): Fact = Fact(
    id = id,
    type = type,
    payload = payload,
    subject = subject,
    appendedAt = appendedAt,
    metadata = metadata,
    tags = tags,
)
