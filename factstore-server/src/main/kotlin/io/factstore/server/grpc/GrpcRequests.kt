package io.factstore.server.grpc

import io.factstore.core.*
import io.factstore.grpc.v1.*
import io.factstore.server.input.*
import io.factstore.core.ReadDirection as CoreReadDirection

/*
 * Parses gRPC input into the specification's requests.
 */

/** Every read states its direction: an unset or unknown direction is invalid input. */
internal fun FactStoreProto.ReadDirection.toDomain(): CoreReadDirection = when (this) {
    FactStoreProto.ReadDirection.READ_DIRECTION_FORWARD -> CoreReadDirection.Forward
    FactStoreProto.ReadDirection.READ_DIRECTION_BACKWARD -> CoreReadDirection.Backward
    else -> throw IllegalArgumentException(
        "A direction is required: READ_DIRECTION_FORWARD or READ_DIRECTION_BACKWARD, but was $this."
    )
}

internal fun FactStoreProto.FactInput.toDomain(): FactInput = FactInput(
    type = type.asFactType(),
    subject = subject.asSubject(),
    payload = payload.toDomain(),
    metadata = metadataMap.asMetadata(),
    tags = tagsMap.asTags()
)

internal fun FactStoreProto.FactPayload.toDomain(): FactPayload = FactPayload(
    data = data.toByteArray(),
)

/**
 * The condition this message sets.
 *
 * A request without a condition appends unconditionally, but a condition that is present and sets
 * no kind is rejected: it is what a client sends whose kind this server does not know, and
 * appending it unconditionally would silently drop the check the client asked for.
 */
internal fun FactStoreProto.AppendCondition.toDomain(): AppendCondition = when (kindCase) {
    FactStoreProto.AppendCondition.KindCase.EXPECTED_LAST_FACT -> AppendCondition.ExpectedLastFact(
        subject = expectedLastFact.subject.asSubject(),
        expectedLastFactId = if (expectedLastFact.hasExpectedLastFactId())
            expectedLastFact.expectedLastFactId.asFactId()
        else null
    )

    FactStoreProto.AppendCondition.KindCase.ALL -> AppendCondition.All(
        conditions = all.conditionsList.map { it.toDomain() }
    )

    FactStoreProto.AppendCondition.KindCase.NONE_MATCH -> AppendCondition.NoneMatch(
        query = noneMatch.query.toDomain(),
        after = if (noneMatch.hasAfterFactId()) noneMatch.afterFactId.asFactId()
        else null
    )

    FactStoreProto.AppendCondition.KindCase.KIND_NOT_SET, null -> throw IllegalArgumentException(
        "An append condition must set one of expected_last_fact, all or none_match."
    )
}

internal fun FactStoreProto.AppendFactsRequest.toDomainRequest(): AppendRequest = parseRequest {
    AppendRequest(
        storeName = storeName.asStoreName(),
        facts = factsList.map { it.toDomain() },
        idempotencyKey = if (hasIdempotencyKey()) IdempotencyKey(idempotencyKey.asUuid()) else IdempotencyKey.generate(),
        condition = if (hasCondition()) condition.toDomain() else AppendCondition.None,
    )
}

internal fun FactStoreProto.CreateStoreRequest.toDomainRequest(): CreateStoreRequest = parseRequest {
    CreateStoreRequest(name.asStoreName())
}

internal fun FactStoreProto.DeleteStoreRequest.toDomainRequest(): RemoveStoreRequest = parseRequest {
    RemoveStoreRequest(name.asStoreName())
}

internal fun FactStoreProto.GetStoreRequest.toDomainRequest(): FindStoreByNameRequest = parseRequest {
    FindStoreByNameRequest(name.asStoreName())
}

internal fun FactStoreProto.GetFactRequest.toDomainRequest(): FindFactByIdRequest = parseRequest {
    FindFactByIdRequest(
        storeName = storeName.asStoreName(),
        factId = factId.asFactId(),
    )
}

internal fun FactStoreProto.FactExistsRequest.toDomainRequest(): ExistsFactByIdRequest = parseRequest {
    ExistsFactByIdRequest(
        storeName = storeName.asStoreName(),
        factId = factId.asFactId(),
    )
}

internal fun FactStoreProto.StreamFactsRequest.toDomainRequest(): StreamFactsRequest = parseRequest {
    StreamFactsRequest(
        storeName = storeName.asStoreName(),
        query = if (hasQuery()) query.toDomain() else null,
        continueAfter = if (hasContinueAfterFactId()) continueAfterFactId.asFactId() else null,
        direction = direction.toDomain(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

internal fun FactStoreProto.FactQueryItem.toDomain(): FactQueryItem = FactQueryItem(
    subjects = subjectsList.map { it.asSubject() }.toSet(),
    types = typesList.map { it.asFactType() }.toSet(),
    tags = tagsMap.asTags(),
)

internal fun FactStoreProto.FactQuery.toDomain(): FactQuery = FactQuery(itemsList.map { it.toDomain() })

internal fun FactStoreProto.SubscribeFactsRequest.toDomainRequest(): SubscribeRequest = parseRequest {
    val startPosition = when (startPositionCase) {
        FactStoreProto.SubscribeFactsRequest.StartPositionCase.FROM_END -> StartPosition.End
        FactStoreProto.SubscribeFactsRequest.StartPositionCase.CONTINUE_AFTER_FACT_ID ->
            StartPosition.ContinueAfter(continueAfterFactId.asFactId())

        else -> StartPosition.Beginning
    }
    SubscribeRequest(
        storeName = storeName.asStoreName(),
        startPosition = startPosition,
    )
}

internal fun FactStoreProto.StoreExistsRequest.toDomainRequest(): ExistsStoreByNameRequest = parseRequest {
    ExistsStoreByNameRequest(name.asStoreName())
}
