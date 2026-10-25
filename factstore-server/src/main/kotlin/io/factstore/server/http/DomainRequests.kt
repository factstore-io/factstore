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
        facts = facts.map { it.toDomain() },
        idempotencyKey = idempotencyKey?.let { IdempotencyKey(it) } ?: IdempotencyKey.generate(),
        condition = condition?.toDomain() ?: AppendCondition.None,
    )
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

internal fun findFactByIdRequest(storeName: String, factId: String): FindFactByIdRequest = parseInput {
    FindFactByIdRequest(storeName.asStoreName(), factId.asFactId())
}

/**
 * Requests the facts of a store, given as query parameters: every fact without criteria, and
 * otherwise the facts matching one query item.
 *
 * Several subjects or types match any of them, while every tag must be carried — the meaning of a
 * single [FactQueryItem]. A query of several items needs a body, and is `POST /facts:query`.
 */
internal fun streamFactsRequest(
    storeName: String,
    subjects: List<String>,
    types: List<String>,
    tags: List<String>,
    continueAfter: String?,
    direction: String?,
    limit: String?,
): StreamFactsRequest = parseInput {
    val criteria = subjects.isNotEmpty() || types.isNotEmpty() || tags.isNotEmpty()
    StreamFactsRequest(
        storeName = storeName.asStoreName(),
        query = if (!criteria) null else FactQuery(
            listOf(
                FactQueryItem(
                    subjects = subjects.map { it.asSubject() }.toSet(),
                    types = types.map { it.asFactType() }.toSet(),
                    tags = tags.asTagFilter(),
                )
            )
        ),
        continueAfter = continueAfter.asFactIdOrNull(),
        direction = direction.asReadDirection(),
        limit = limit.asLimit(),
    )
}

internal fun QueryFactsHttpRequest.toDomainRequest(storeName: String): StreamFactsRequest = parseInput {
    StreamFactsRequest(
        storeName = storeName.asStoreName(),
        query = FactQuery(items.map { it.toDomain() }),
        continueAfter = continueAfter?.let { FactId(it) },
        direction = direction.asReadDirection(),
        limit = limit.asLimit(),
    )
}

internal fun FactQueryItemHttp.toDomain(): FactQueryItem = FactQueryItem(
    subjects = subjects?.map { it.asSubject() }?.toSet() ?: emptySet(),
    types = types?.map { it.asFactType() }?.toSet() ?: emptySet(),
    tags = tags?.asTags() ?: emptyMap(),
)

internal fun subscribeRequest(
    storeName: String,
    from: String?,
    continueAfter: String?,
): SubscribeRequest = parseInput {
    require(from == null || continueAfter == null) {
        "'from' and 'continueAfter' cannot be combined: a subscription starts at one position."
    }
    val startPosition = when (from?.trim()?.lowercase()) {
        "beginning" -> StartPosition.Beginning
        "end" -> StartPosition.End
        null -> continueAfter?.let { StartPosition.ContinueAfter(it.asFactId()) } ?: StartPosition.Beginning
        else -> throw IllegalArgumentException("'from' must be 'beginning' or 'end', but was '$from'.")
    }
    SubscribeRequest(storeName.asStoreName(), startPosition)
}

// ─── The HTTP shapes as the specification's values ───────────────────────────

internal fun AppendConditionHttp.toDomain(): AppendCondition =
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
                conditions = conditions.map { it.toDomain() }
            )

        is AppendConditionHttp.NoneMatch ->
            AppendCondition.NoneMatch(
                query = FactQuery(query.items.map { it.toDomain() }),
                after = after?.toFactId(),
            )
    }

internal fun FactInputHttp.toDomain() = FactInput(
    type = type.asFactType(),
    payload = FactPayload(data = payload.data),
    subject = subject.asSubject(),
    metadata = metadata?.asMetadata() ?: emptyMap(),
    tags = tags?.asTags() ?: emptyMap(),
)
