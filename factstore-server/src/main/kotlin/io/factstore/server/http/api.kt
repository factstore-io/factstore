package io.factstore.server.http

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.time.Instant
import java.util.UUID

data class AppendHttpRequest(
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
        val subjectRef: SubjectRefHttp,
        val expectedLastFactId: UUID?
    ) : AppendConditionHttp

    data class ExpectedMultiSubjectLastFact(
        val expectations: Map<SubjectRefHttp, UUID?>
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
    val type: String,
    val subjectRef: SubjectRefHttp,
    val appendedAt: Instant?,
    val payload: FactPayloadHttp,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap()
)

data class FactPayloadHttp(
    val data: ByteArray,
)

data class SubjectRefHttp(
    val type: String,
    val id: String
)
