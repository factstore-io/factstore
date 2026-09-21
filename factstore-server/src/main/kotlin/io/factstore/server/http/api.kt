package io.factstore.server.http

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.factstore.core.AppendRequest
import io.factstore.core.CLEAN_TEXT_PATTERN
import io.factstore.core.FactFilter
import io.factstore.core.FactInput
import io.factstore.core.FactQuery
import io.factstore.core.FactPayload
import io.factstore.core.FactType
import io.factstore.core.StoreName
import io.factstore.core.Subject
import org.eclipse.microprofile.openapi.annotations.media.Schema
import java.time.Instant
import java.util.*

data class AppendHttpRequest(
    @field:Schema(minItems = 1, maxItems = AppendRequest.MAX_FACTS)
    val facts: List<FactInputHttp>,
    val idempotencyKey: UUID? = null,
    val condition: AppendConditionHttp? = null
)

data class FactInputHttp(
    @field:Schema(maxLength = FactType.MAX_LENGTH, pattern = CLEAN_TEXT_PATTERN)
    val type: String,
    @field:Schema(maxLength = Subject.MAX_LENGTH, pattern = CLEAN_TEXT_PATTERN)
    val subject: String,
    val payload: FactPayloadHttp,
    @field:Schema(maxProperties = FactInput.MAX_METADATA_ENTRIES)
    val metadata: Map<String, String>? = null,
    @field:Schema(maxProperties = FactInput.MAX_TAGS)
    val tags: Map<String, String>? = null,
)

data class AppendedHttp(
    val factIds: List<UUID>,
    val appendedAt: Instant,
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(
        value = AppendConditionHttp.None::class,
        name = "none"
    ),
    JsonSubTypes.Type(
        value = AppendConditionHttp.ExpectedLastFact::class,
        name = "expectedLastFact"
    ),
    JsonSubTypes.Type(
        value = AppendConditionHttp.All::class,
        name = "all"
    ),
    JsonSubTypes.Type(
        value = AppendConditionHttp.TagQueryBased::class,
        name = "tagQueryBased"
    )
)
sealed interface AppendConditionHttp {

    data object None : AppendConditionHttp

    data class ExpectedLastFact(
        val subject: String,
        val expectedLastFactId: UUID?
    ) : AppendConditionHttp

    data class All(
        val conditions: List<AppendConditionHttp>
    ) : AppendConditionHttp

    data class TagQueryBased(
        val failIfEventsMatch: FactQueryHttp,
        val after: UUID?
    ) : AppendConditionHttp
}

data class FactQueryHttp(
    @field:Schema(minItems = 1)
    val queryItems: List<TagQueryItemHttp>
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(
        value = TagQueryItemHttp.TagOnly::class,
        name = "tagOnly"
    ),
    JsonSubTypes.Type(
        value = TagQueryItemHttp.TagType::class,
        name = "tagType"
    )
)
sealed interface TagQueryItemHttp {

    data class TagType(
        @field:Schema(minItems = 1)
        val types: List<String>,
        @field:Schema(minProperties = 1, maxProperties = FactInput.MAX_TAGS)
        val tags: Map<String, String>
    ) : TagQueryItemHttp

    data class TagOnly(
        @field:Schema(minProperties = 1, maxProperties = FactInput.MAX_TAGS)
        val tags: Map<String, String>
    ) : TagQueryItemHttp
}

data class FactHttp(
    val id: UUID?,
    val type: String,
    val subject: String,
    val appendedAt: Instant?,
    val payload: FactPayloadHttp,
    val metadata: Map<String, String>?,
    val tags: Map<String, String>?
)

/**
 * A query and how to read its result, the body of `POST /facts:query`.
 *
 * @property filters the filters to match, of which at least one must match a fact
 * @property direction `forward` (the default) or `backward`
 * @property limit the maximum number of facts to stream, over the whole result
 */
data class StreamFactsByQueryHttpRequest(
    @field:Schema(minItems = 1, maxItems = FactQuery.MAX_FILTERS)
    val filters: List<FactFilterHttp>,
    @field:Schema(enumeration = ["forward", "backward"], defaultValue = "forward")
    val direction: String? = null,
    @field:Schema(minimum = "1")
    val limit: Int? = null,
)

/**
 * Matches facts by subject, type and tags. Every property that is set must hold, while a property
 * with several values matches any of them. At least one must be set.
 */
data class FactFilterHttp(
    @field:Schema(maxItems = FactFilter.MAX_SUBJECTS)
    val subjects: List<String>? = null,
    @field:Schema(maxItems = FactFilter.MAX_TYPES)
    val types: List<String>? = null,
    @field:Schema(maxProperties = FactInput.MAX_TAGS)
    val tags: Map<String, String>? = null,
)

/**
 * One line of an NDJSON fact stream.
 *
 * Each line is a JSON object with exactly one property: `fact` for every fact, followed by
 * either `end` once all facts were sent, or `error` if the stream failed part way. A stream
 * that stops without an `end` or `error` line is incomplete, however the connection ended.
 */
sealed interface FactStreamLineHttp {

    data class FactLine(val fact: FactHttp) : FactStreamLineHttp

    data class EndLine(val end: FactStreamEndHttp) : FactStreamLineHttp

    data class ErrorLine(val error: ApiError) : FactStreamLineHttp
}

/** Closes a complete fact stream; [count] is the number of `fact` lines before it. */
data class FactStreamEndHttp(val count: Long)

data class FactPayloadHttp(
    @field:Schema(description = "Base64-encoded payload of at most ${FactPayload.MAX_SIZE} bytes.")
    val data: ByteArray,
)

data class CreateStoreHttpRequest(
    @field:Schema(maxLength = StoreName.MAX_LENGTH, pattern = StoreName.REGEX_PATTERN)
    val name: String
)

data class StoreMetadataHttp(
    val id: UUID,
    val name: String,
    val createdAt: Instant
)
