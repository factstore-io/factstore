package io.factstore.server.http

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.factstore.core.StoreName
import io.factstore.server.http.validation.ValidStoreName
import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import java.time.Instant
import java.util.UUID

data class AppendHttpRequest(
    @field:NotEmpty
    @field:Valid
    val facts: List<FactHttp>,
    val idempotencyKey: UUID? = null,
    val condition: AppendConditionHttp? = null
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
        value = AppendConditionHttp.ExpectedMultiSubjectLastFact::class,
        name = "expectedMultiSubjectLastFact"
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

    data class ExpectedMultiSubjectLastFact(
        val expectations: Map<String, UUID?>
    ) : AppendConditionHttp

    data class TagQueryBased(
        val failIfEventsMatch: FactQueryHttp,
        val after: UUID?
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
