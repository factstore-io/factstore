package io.factstore.server.http

import jakarta.ws.rs.core.Response
import io.factstore.core.AppendCondition
import io.factstore.core.AppendRequest
import io.factstore.core.AppendResult
import io.factstore.core.Fact
import io.factstore.core.FactFilter
import io.factstore.core.FactInput
import io.factstore.core.FactPayload
import io.factstore.core.FactQuery
import io.factstore.core.FactQueryResult
import io.factstore.core.IdempotencyKey
import io.factstore.core.Limit
import io.factstore.core.ReadDirection
import io.factstore.core.StoreName
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagOnlyQueryItem
import io.factstore.core.TagQuery
import io.factstore.core.TagQueryItem
import io.factstore.core.TagTypeItem
import io.factstore.core.TagValue
import io.factstore.core.TimeRange
import io.factstore.core.toFactId
import io.factstore.core.toFactType
import io.factstore.core.toTagKey
import io.factstore.core.toTagValue
import io.factstore.server.http.StreamApiException.FactNotFoundException
import io.factstore.server.http.StreamApiException.StoreNotFoundException
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform


fun AppendResult.toResponse(): Response = when(this) {
    is AppendResult.Appended -> Response.ok(AppendedHttp(factIds.map { it.uuid }, appendedAt)).build()
    is AppendResult.AlreadyApplied ->  Response.ok().build()
    is AppendResult.AppendConditionViolated -> appendConditionViolatedError()
    is AppendResult.StoreNotFound -> storeNotFoundError(storeName)
}

fun AppendHttpRequest.toAppendRequest(storeName: StoreName): AppendRequest = AppendRequest(
    storeName = storeName,
    facts = facts.toFactInputs(),
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

        is AppendConditionHttp.All ->
            AppendCondition.All(
                conditions = conditions.map { it.toAppendCondition() }
            )

        is AppendConditionHttp.TagQueryBased ->
            AppendCondition.TagQueryBased(
                failIfEventsMatch = failIfEventsMatch.toTagQuery(),
                after = after?.toFactId()
            )

        is AppendConditionHttp.IfNoneMatch ->
            AppendCondition.IfNoneMatch(
                filter = filter.toDomainPredicate(),
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
                tags = tags.entries.associate { (k, v) ->
                    k.toTagKey() to v.toTagValue()
                }
            )

        is TagQueryItemHttp.TagType ->
            TagTypeItem(
                types = types.map { it.toFactType() }.toSet(),
                tags = tags.entries.associate { (k, v) ->
                    k.toTagKey() to v.toTagValue()
                }
            )
    }


fun List<FactInputHttp>.toFactInputs() = map { it.toFactInput() }

fun FactInputHttp.toFactInput() = FactInput(
    type = type.toFactType(),
    payload = payload.toPayload(),
    subject = Subject(subject),
    metadata = metadata ?: emptyMap(),
    tags = tags?.entries?.associate { Pair(it.key.toTagKey(), it.value.toTagValue()) } ?: emptyMap()
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

// ── FactFilter HTTP → domain ──────────────────────────────────────────────────

fun StreamFactsRequestHttp.toDomainQuery(storeName: StoreName): FactQuery = FactQuery(
    storeName = storeName,
    filter = filter?.toDomain() ?: FactFilter.All,
    limit = limit?.let { Limit.of(it) } ?: Limit.None,
    direction = when (direction?.lowercase()) {
        "backward" -> ReadDirection.Backward
        else -> ReadDirection.Forward
    },
    cursor = cursor?.toFactId(),
)

fun FactFilterHttp.toDomain(): FactFilter = when (this) {
    is FactFilterHttp.All -> FactFilter.All
    else -> (this as FactFilterHttp).toDomainPredicate()
}

fun FactFilterHttp.toDomainPredicate(): FactFilter.Predicate = when (this) {
    is FactFilterHttp.All -> throw IllegalArgumentException("'all' cannot appear inside a predicate context")
    is FactFilterHttp.Subject -> FactFilter.Predicate.Subject(io.factstore.core.Subject(value))
    is FactFilterHttp.SubjectPrefix -> FactFilter.Predicate.SubjectPrefix(prefix)
    is FactFilterHttp.Type -> FactFilter.Predicate.Type(value.toFactType())
    is FactFilterHttp.Tag -> FactFilter.Predicate.Tag(TagKey(key), TagValue(value))
    is FactFilterHttp.Metadata -> FactFilter.Predicate.Metadata(key, value)
    is FactFilterHttp.TimeRange -> FactFilter.Predicate.TimeRange(TimeRange(from, to))
    is FactFilterHttp.AnyOf -> FactFilter.Predicate.AnyOf(predicates.map { it.toDomainPredicate() })
    is FactFilterHttp.AllOf -> FactFilter.Predicate.AllOf(predicates.map { it.toDomainPredicate() })
    is FactFilterHttp.First -> FactFilter.Predicate.First(n, predicate.toDomainPredicate())
    is FactFilterHttp.Last -> FactFilter.Predicate.Last(n, predicate.toDomainPredicate())
}

fun FactQueryResult.toResponse(): Flow<FactHttp> = when (this) {
    is FactQueryResult.StoreNotFound -> throw StoreNotFoundException(storeName)
    is FactQueryResult.CursorNotFound -> throw FactNotFoundException(factId)
    is FactQueryResult.FactStream -> stream.transform { batch -> batch.forEach { emit(it.toFactHttp()) } }
}
