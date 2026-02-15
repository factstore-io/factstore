package org.factstore.server.http

import jakarta.ws.rs.core.Response
import org.factstore.core.AppendCondition
import org.factstore.core.AppendRequest
import org.factstore.core.AppendResult
import org.factstore.core.Fact
import org.factstore.core.FactId
import org.factstore.core.FactPayload
import org.factstore.core.IdempotencyKey
import org.factstore.core.SubjectRef
import org.factstore.core.TagOnlyQueryItem
import org.factstore.core.TagQuery
import org.factstore.core.TagQueryItem
import org.factstore.core.TagTypeItem
import org.factstore.core.toFactId
import org.factstore.core.toFactType
import org.factstore.core.toTagKey
import org.factstore.core.toTagValue
import java.time.Instant


fun AppendResult.toResponse(): Response {
    return Response.ok(this::class.qualifiedName).build()
}

fun AppendHttpRequest.toAppendRequest(): AppendRequest = AppendRequest(
    facts = facts.toFacts(),
    idempotencyKey = idempotencyKey?.let { IdempotencyKey(it) } ?: IdempotencyKey(),
    condition = condition?.toAppendCondition() ?: AppendCondition.None
)

fun AppendConditionHttp.toAppendCondition(): AppendCondition =
    when (this) {
        is AppendConditionHttp.None ->
            AppendCondition.None

        is AppendConditionHttp.ExpectedLastFact ->
            AppendCondition.ExpectedLastFact(
                subjectRef = subjectRef.toSubjectRef(),
                expectedLastFactId = expectedLastFactId?.toFactId()
            )

        is AppendConditionHttp.ExpectedMultiSubjectLastFact ->
            AppendCondition.ExpectedMultiSubjectLastFact(
                expectations = expectations.mapKeys { (subject, _) ->
                    subject.toSubjectRef()
                }.mapValues { (_, factId) ->
                    factId?.toFactId()
                }
            )

        is AppendConditionHttp.TagQueryBased ->
            AppendCondition.TagQueryBased(
                failIfEventsMatch = failIfEventsMatch.toTagQuery(),
                after = after?.toFactId()
            )
    }

fun FactQueryHttp.toTagQuery(): TagQuery =
    TagQuery(
        queryItems = queryItems.map { it.toTagQueryItem() }
    )

fun TagQueryItemHttp.toTagQueryItem(): TagQueryItem =
    when (this) {
        is TagQueryItemHttp.TagOnly ->
            TagOnlyQueryItem(
                tags = tags.entries.map { (k, v) ->
                    k.toTagKey() to v.toTagValue()
                }
            )

        is TagQueryItemHttp.TagType ->
            TagTypeItem(
                types = types.map { it.toFactType() },
                tags = tags.entries.map { (k, v) ->
                    k.toTagKey() to v.toTagValue()
                }
            )
    }


fun List<FactHttp>.toFacts() = map { it.toFact() }

fun FactHttp.toFact() = Fact(
    id = id?.toFactId() ?: FactId.generate(),
    type = type.toFactType(),
    payload = payload.toPayload(),
    subjectRef = subjectRef.toSubjectRef(),
    appendedAt = appendedAt ?: Instant.now(),
    metadata = metadata,
    tags = tags.entries.associate { Pair(it.key.toTagKey(), it.value.toTagValue()) }
)

private fun FactPayloadHttp.toPayload(): FactPayload = FactPayload(
    data = data,
)

private fun SubjectRefHttp.toSubjectRef() = SubjectRef(
    type = type,
    id = id
)

fun Fact.toFactHttp() = FactHttp(
    id = id.uuid,
    type = type.value,
    subjectRef = subjectRef.toSubjectRefHttp(),
    appendedAt = appendedAt,
    payload = payload.toFactPayloadHttp(),
    metadata = metadata,
    tags = tags.entries.associate { Pair(it.key.value, it.value.value) }
)

fun SubjectRef.toSubjectRefHttp() = SubjectRefHttp(
    type = type,
    id = id
)

fun FactPayload.toFactPayloadHttp() = FactPayloadHttp(
    data = data
)
