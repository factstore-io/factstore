package io.factstore.server.http

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.factstore.server.http.validation.ValidStoreName
import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotEmpty
import java.time.Instant
import java.util.*

// ── FactFilter HTTP DTOs ──────────────────────────────────────────────────────

data class StreamFactsRequestHttp(
    val filter: FactFilterHttp? = null,
    val limit: Int? = null,
    val direction: String? = null,
    val cursor: UUID? = null,
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = FactFilterHttp.All::class,           name = "all"),
    JsonSubTypes.Type(value = FactFilterHttp.Subject::class,       name = "subject"),
    JsonSubTypes.Type(value = FactFilterHttp.SubjectPrefix::class, name = "subjectPrefix"),
    JsonSubTypes.Type(value = FactFilterHttp.Type::class,          name = "type"),
    JsonSubTypes.Type(value = FactFilterHttp.Tag::class,           name = "tag"),
    JsonSubTypes.Type(value = FactFilterHttp.Metadata::class,      name = "metadata"),
    JsonSubTypes.Type(value = FactFilterHttp.TimeRange::class,     name = "timeRange"),
    JsonSubTypes.Type(value = FactFilterHttp.AnyOf::class,         name = "anyOf"),
    JsonSubTypes.Type(value = FactFilterHttp.AllOf::class,         name = "allOf"),
    JsonSubTypes.Type(value = FactFilterHttp.First::class,         name = "first"),
    JsonSubTypes.Type(value = FactFilterHttp.Last::class,          name = "last"),
)
sealed interface FactFilterHttp {
    data object All : FactFilterHttp
    data class Subject(val value: String) : FactFilterHttp
    data class SubjectPrefix(val prefix: String) : FactFilterHttp
    data class Type(val value: String) : FactFilterHttp
    data class Tag(val key: String, val value: String) : FactFilterHttp
    data class Metadata(val key: String, val value: String) : FactFilterHttp
    data class TimeRange(val from: Instant? = null, val to: Instant? = null) : FactFilterHttp
    data class AnyOf(val predicates: List<FactFilterHttp>) : FactFilterHttp
    data class AllOf(val predicates: List<FactFilterHttp>) : FactFilterHttp
    data class First(val n: Int, val predicate: FactFilterHttp) : FactFilterHttp
    data class Last(val n: Int, val predicate: FactFilterHttp) : FactFilterHttp
}

data class AppendHttpRequest(
    @field:NotEmpty
    val facts: List<@Valid FactInputHttp>,
    val idempotencyKey: UUID? = null,
    val condition: AppendConditionHttp? = null
)

data class FactInputHttp(
    @field:NotBlank
    val type: String,
    @field:NotBlank
    val subject: String,
    @field:Valid
    val payload: FactPayloadHttp,
    val metadata: Map<String, String>? = null,
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
    ),
    JsonSubTypes.Type(
        value = AppendConditionHttp.IfNoneMatch::class,
        name = "ifNoneMatch"
    ),
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

    data class IfNoneMatch(
        val filter: FactFilterHttp,
        val after: UUID? = null,
    ) : AppendConditionHttp
}

data class FactQueryHttp(
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
        val types: List<String>,
        val tags: Map<String, String>
    ) : TagQueryItemHttp

    data class TagOnly(
        val tags: Map<String, String>
    ) : TagQueryItemHttp
}

data class FactHttp(
    val id: UUID?,
    @field:NotBlank
    val type: String,
    @field:NotBlank
    val subject: String,
    val appendedAt: Instant?,
    @field:Valid
    val payload: FactPayloadHttp,
    val metadata: Map<String, String>?,
    val tags: Map<String, String>?
)

data class FactPayloadHttp(
    @field:NotEmpty
    val data: ByteArray,
)

data class CreateStoreHttpRequest(
    @field:ValidStoreName
    val name: String
)

data class StoreMetadataHttp(
    val id: UUID,
    val name: String,
    val createdAt: Instant
)
