package io.factstore.server.http

import jakarta.ws.rs.core.Response
import io.factstore.core.AppendCondition
import io.factstore.core.AppendRequest
import io.factstore.core.AppendResult
import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.FactPayload
import io.factstore.core.IdempotencyKey
import io.factstore.core.StoreName
import io.factstore.core.Subject
import io.factstore.core.TagOnlyQueryItem
import io.factstore.core.TagQuery
import io.factstore.core.TagQueryItem
import io.factstore.core.TagTypeItem
import io.factstore.core.toFactId
import io.factstore.core.toFactType
import io.factstore.core.toTagKey
import io.factstore.core.toTagValue
import java.time.Instant


fun AppendResult.toResponse(): Response = when(this) {
    is AppendResult.Appended -> Response.ok().build()
    is AppendResult.AlreadyApplied ->  Response.ok().build()
    is AppendResult.AppendConditionViolated -> appendConditionViolatedError()
    is AppendResult.StoreNotFound -> storeNotFoundError(storeName)
    is AppendResult.DuplicateFactIds -> duplicateFactIdsError(factIds)
}

fun AppendHttpRequest.toAppendRequest(storeName: StoreName): AppendRequest = AppendRequest(
    storeName = storeName,
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
                subject = Subject(subject),
                expectedLastFactId = expectedLastFactId?.toFactId()
            )

        is AppendConditionHttp.ExpectedMultiSubjectLastFact ->
            AppendCondition.ExpectedMultiSubjectLastFact(
                expectations = expectations.mapKeys { (subject, _) ->
                    Subject(subject)
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
    subject = Subject(subject),
    appendedAt = appendedAt ?: Instant.now(),
    metadata = metadata,
    tags = tags.entries.associate { Pair(it.key.toTagKey(), it.value.toTagValue()) }
)

private fun FactPayloadHttp.toPayload(): FactPayload = FactPayload(
    data = data,
)

fun Fact.toFactHttp() = FactHttp(
    id = id.uuid,
    type = type.value,
    subject = subject.value,
    appendedAt = appendedAt,
    payload = payload.toFactPayloadHttp(),
    metadata = metadata,
    tags = tags.entries.associate { Pair(it.key.value, it.value.value) }
)

fun FactPayload.toFactPayloadHttp() = FactPayloadHttp(
    data = data
)
