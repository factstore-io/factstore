package io.factstore.server.http

import io.factstore.core.*
import io.factstore.server.input.*

/*
 * Parses HTTP input into the specification's requests.
 *
 * Each function returns a valid request or fails with InvalidInputException, which the transport
 * reports as 400. The catch sits inside these functions and never around the store call, so a
 * failure while executing a request stays a 500 instead of being blamed on the client.
 */

internal fun AppendHttpRequest.toDomainRequest(storeName: String): AppendRequest = parseInput {
    AppendRequest(
        storeName = storeName.asStoreName(),
        facts = facts.toFactInputs(),
        idempotencyKey = idempotencyKey?.let { IdempotencyKey(it) } ?: IdempotencyKey(),
        condition = condition?.toAppendCondition() ?: AppendCondition.None,
    )
}

internal fun FactQueryHttp.toDomainRequest(storeName: String): FindByTagQueryRequest = parseInput {
    FindByTagQueryRequest(storeName.asStoreName(), toTagQuery())
}

internal fun CreateStoreHttpRequest.toDomainRequest(): CreateStoreRequest = parseInput {
    CreateStoreRequest(name.asStoreName())
}

internal fun findStoreByNameRequest(storeName: String): FindStoreByNameRequest = parseInput {
    FindStoreByNameRequest(storeName.asStoreName())
}

internal fun existsStoreByNameRequest(storeName: String): ExistsStoreByNameRequest = parseInput {
    ExistsStoreByNameRequest(storeName.asStoreName())
}

internal fun removeStoreRequest(storeName: String): RemoveStoreRequest = parseInput {
    RemoveStoreRequest(storeName.asStoreName())
}

internal fun findByIdRequest(storeName: String, factId: String): FindByIdRequest = parseInput {
    FindByIdRequest(storeName.asStoreName(), factId.asFactId())
}

internal fun streamFactsRequest(
    storeName: String,
    direction: String?,
    limit: String?,
): StreamFactsRequest = parseInput {
    StreamFactsRequest(
        storeName = storeName.asStoreName(),
        direction = direction.asReadDirection(),
        limit = limit.asLimit(),
    )
}

internal fun StreamFactsByQueryHttpRequest.toDomainRequest(storeName: String): StreamFactsByQueryRequest = parseInput {
    StreamFactsByQueryRequest(
        storeName = storeName.asStoreName(),
        query = FactQuery(filters.map { it.toFactFilter() }),
        direction = direction.asReadDirection(),
        limit = limit.asLimit(),
    )
}

internal fun FactFilterHttp.toFactFilter(): FactFilter = FactFilter(
    subjects = subjects?.map { it.asSubject() }?.toSet() ?: emptySet(),
    types = types?.map { it.asFactType() }?.toSet() ?: emptySet(),
    tags = tags?.asTags() ?: emptyMap(),
)

internal fun streamFactsByTagsRequest(
    storeName: String,
    tags: List<String>,
    from: String?,
    to: String?,
    direction: String?,
    limit: String?,
): StreamFactsByTagsRequest = parseInput {
    require(from == null && to == null) { "Combining tag filters with time range is not yet supported." }
    StreamFactsByTagsRequest(
        storeName = storeName.asStoreName(),
        tags = tags.asTagFilter(),
        direction = direction.asReadDirection(),
        limit = limit.asLimit(),
    )
}

internal fun streamFactsByTypeRequest(
    storeName: String,
    type: String,
    direction: String?,
    limit: String?,
): StreamFactsByTypeRequest = parseInput {
    StreamFactsByTypeRequest(
        storeName = storeName.asStoreName(),
        type = type.asFactType(),
        direction = direction.asReadDirection(),
        limit = limit.asLimit(),
    )
}

internal fun streamFactsBySubjectRequest(
    storeName: String,
    subject: String,
    direction: String?,
    limit: String?,
): StreamFactsBySubjectRequest = parseInput {
    StreamFactsBySubjectRequest(
        storeName = storeName.asStoreName(),
        subject = subject.asSubject(),
        direction = direction.asReadDirection(),
        limit = limit.asLimit(),
    )
}

internal fun findByTagsRequest(
    storeName: String,
    tags: List<String>,
    from: String?,
    to: String?,
    limit: String?,
    direction: String?,
): FindByTagsRequest = parseInput {
    require(from == null && to == null) { "Combining tag filters with time range is not yet supported." }
    FindByTagsRequest(
        storeName = storeName.asStoreName(),
        tags = tags.asTagFilter(),
        limit = limit.asLimit(),
        direction = direction.asReadDirection(),
    )
}

internal fun findInTimeRangeRequest(
    storeName: String,
    from: String?,
    to: String?,
    limit: String?,
    direction: String?,
): FindInTimeRangeRequest = parseInput {
    FindInTimeRangeRequest(
        storeName = storeName.asStoreName(),
        timeRange = TimeRange(start = from.asInstant(), end = to.asInstant()),
        limit = limit.asLimit(),
        direction = direction.asReadDirection(),
    )
}

internal fun subscribeRequest(storeName: String, after: String?, from: String?): SubscribeRequest = parseInput {
    val startPosition = when (from?.trim()?.lowercase()) {
        "beginning" -> StartPosition.Beginning
        "end" -> StartPosition.End
        null -> after?.let { StartPosition.After(it.asFactId()) } ?: StartPosition.Beginning
        else -> throw IllegalArgumentException("'from' must be 'beginning' or 'end', but was '$from'.")
    }
    SubscribeRequest(storeName.asStoreName(), startPosition)
}

internal fun replayRequest(storeName: String, after: String?): ReplayRequest = parseInput {
    ReplayRequest(
        storeName = storeName.asStoreName(),
        start = after?.let { ReplayStart.After(it.asFactId()) } ?: ReplayStart.Beginning,
    )
}

// ─── The HTTP shapes as the specification's values ───────────────────────────

internal fun AppendConditionHttp.toAppendCondition(): AppendCondition =
    when (this) {
        is AppendConditionHttp.None ->
            AppendCondition.None

        is AppendConditionHttp.ExpectedLastFact ->
            AppendCondition.ExpectedLastFact(
                subject = subject.asSubject(),
                expectedLastFactId = expectedLastFactId?.toFactId(),
            )

        is AppendConditionHttp.All ->
            AppendCondition.All(
                conditions = conditions.map { it.toAppendCondition() }
            )

        is AppendConditionHttp.TagQueryBased ->
            AppendCondition.TagQueryBased(
                failIfEventsMatch = failIfEventsMatch.toTagQuery(),
                after = after?.toFactId(),
            )
    }

internal fun FactQueryHttp.toTagQuery(): TagQuery =
    TagQuery(queryItems = queryItems.map { it.toTagQueryItem() })

internal fun TagQueryItemHttp.toTagQueryItem(): TagQueryItem =
    when (this) {
        is TagQueryItemHttp.TagOnly ->
            TagOnlyQueryItem(tags = tags.asTags())

        is TagQueryItemHttp.TagType ->
            TagTypeItem(
                types = types.map { it.asFactType() }.toSet(),
                tags = tags.asTags(),
            )
    }

internal fun List<FactInputHttp>.toFactInputs() = map { it.toFactInput() }

internal fun FactInputHttp.toFactInput() = FactInput(
    type = type.asFactType(),
    payload = FactPayload(data = payload.data),
    subject = subject.asSubject(),
    metadata = metadata?.asMetadata() ?: emptyMap(),
    tags = tags?.asTags() ?: emptyMap(),
)
