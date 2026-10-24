package io.factstore.server.grpc

import com.google.protobuf.Timestamp
import io.factstore.core.*
import io.factstore.grpc.v1.*
import io.factstore.server.input.*
import java.time.Instant
import io.factstore.core.ReadDirection as CoreReadDirection

/*
 * Parses gRPC input into the specification's requests.
 */

internal fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

/** Every read states its direction: an unset or unknown direction is invalid input. */
internal fun FactStoreProto.ReadDirection.toCore(): CoreReadDirection = when (this) {
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

typealias GrpcAppendRequest = FactStoreProto.AppendFactsRequest

internal fun GrpcAppendRequest.toDomainRequest(): AppendRequest = parseRequest {
    AppendRequest(
        storeName = storeName.asStoreName(),
        facts = factsList.map { it.toDomain() },
        idempotencyKey = if (hasIdempotencyKey()) IdempotencyKey(idempotencyKey.asUuid()) else IdempotencyKey(),
        condition = if (hasCondition()) condition.toDomain() else AppendCondition.None,
    )
}

typealias GrpcCreateStoreRequest = FactStoreProto.CreateStoreRequest

internal fun GrpcCreateStoreRequest.toDomainRequest(): CreateStoreRequest = parseRequest {
    CreateStoreRequest(name.asStoreName())
}

typealias GrpcDeleteStoreRequest = FactStoreProto.DeleteStoreRequest

internal fun GrpcDeleteStoreRequest.toDomainRequest(): RemoveStoreRequest = parseRequest {
    RemoveStoreRequest(name.asStoreName())
}

typealias GrpcFindStoreByNameRequest = FactStoreProto.GetStoreRequest

internal fun GrpcFindStoreByNameRequest.toDomainRequest(): FindStoreByNameRequest = parseRequest {
    FindStoreByNameRequest(name.asStoreName())
}

typealias GrpcGetFactRequest = FactStoreProto.GetFactRequest

internal fun GrpcGetFactRequest.toDomainRequest(): FindByIdRequest = parseRequest {
    FindByIdRequest(
        storeName = storeName.asStoreName(),
        factId = factId.asFactId(),
    )
}

typealias GrpcFactExistsRequest = FactStoreProto.FactExistsRequest

internal fun GrpcFactExistsRequest.toDomainRequest(): ExistsByIdRequest = parseRequest {
    ExistsByIdRequest(
        storeName = storeName.asStoreName(),
        factId = factId.asFactId(),
    )
}

typealias GrpcStreamFactsRequest = FactStoreProto.StreamFactsRequest

internal fun GrpcStreamFactsRequest.toDomainRequest(): StreamFactsRequest = parseRequest {
    StreamFactsRequest(
        storeName = storeName.asStoreName(),
        continueAfter = if (hasContinueAfterFactId()) continueAfterFactId.asFactId() else null,
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

internal fun FactStoreProto.FactFilter.toDomain(): FactFilter = FactFilter(
    subjects = subjectsList.map { it.asSubject() }.toSet(),
    types = typesList.map { it.asFactType() }.toSet(),
    tags = tagsMap.asTags(),
)

internal fun FactStoreProto.FactQuery.toDomain(): FactQuery = FactQuery(filtersList.map { it.toDomain() })

typealias GrpcStreamFactsByQueryRequest = FactStoreProto.StreamFactsByQueryRequest

internal fun GrpcStreamFactsByQueryRequest.toDomainRequest(): StreamFactsByQueryRequest = parseRequest {
    StreamFactsByQueryRequest(
        storeName = storeName.asStoreName(),
        query = query.toDomain(),
        continueAfter = if (hasContinueAfterFactId()) continueAfterFactId.asFactId() else null,
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcStreamFactsByTagsRequest = FactStoreProto.StreamFactsByTagsRequest

internal fun GrpcStreamFactsByTagsRequest.toDomainRequest(): StreamFactsByTagsRequest = parseRequest {
    StreamFactsByTagsRequest(
        storeName = storeName.asStoreName(),
        tags = tagsMap.asTags(),
        continueAfter = if (hasContinueAfterFactId()) continueAfterFactId.asFactId() else null,
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcStreamFactsByTypeRequest = FactStoreProto.StreamFactsByTypeRequest

internal fun GrpcStreamFactsByTypeRequest.toDomainRequest(): StreamFactsByTypeRequest = parseRequest {
    StreamFactsByTypeRequest(
        storeName = storeName.asStoreName(),
        type = type.asFactType(),
        continueAfter = if (hasContinueAfterFactId()) continueAfterFactId.asFactId() else null,
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcStreamFactsBySubjectRequest = FactStoreProto.StreamFactsBySubjectRequest

internal fun GrpcStreamFactsBySubjectRequest.toDomainRequest(): StreamFactsBySubjectRequest = parseRequest {
    StreamFactsBySubjectRequest(
        storeName = storeName.asStoreName(),
        subject = subject.asSubject(),
        continueAfter = if (hasContinueAfterFactId()) continueAfterFactId.asFactId() else null,
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcSubscribeFactsRequest = FactStoreProto.SubscribeFactsRequest

internal fun GrpcSubscribeFactsRequest.toDomainRequest(): SubscribeRequest = parseRequest {
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

typealias GrpcExistsStoreRequest = FactStoreProto.StoreExistsRequest

internal fun GrpcExistsStoreRequest.toDomainRequest(): ExistsStoreByNameRequest = parseRequest {
    ExistsStoreByNameRequest(name.asStoreName())
}
