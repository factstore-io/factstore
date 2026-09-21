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

    FactStoreProto.AppendCondition.KindCase.TAG_QUERY_BASED -> AppendCondition.TagQueryBased(
        failIfEventsMatch = tagQueryBased.failIfEventsMatch.toDomain(),
        after = if (tagQueryBased.hasAfterFactId()) tagQueryBased.afterFactId.asFactId()
        else null
    )

    else -> AppendCondition.None
}

internal fun FactStoreProto.TagQuery.toDomain(): TagQuery = TagQuery(
    queryItems = itemsList.map { it.toDomain() }
)

internal fun FactStoreProto.TagQueryItem.toDomain(): TagQueryItem = when (kindCase) {
    FactStoreProto.TagQueryItem.KindCase.TAG_ONLY -> TagOnlyQueryItem(
        tags = tagOnly.tagsMap.asTags()
    )

    FactStoreProto.TagQueryItem.KindCase.TAG_TYPE -> TagTypeItem(
        types = tagType.typesList.map { it.asFactType() }.toSet(),
        tags = tagType.tagsMap.asTags()
    )

    else -> throw IllegalArgumentException("TagQueryItem has no kind set")
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
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcStreamFactsByTagsRequest = FactStoreProto.StreamFactsByTagsRequest

internal fun GrpcStreamFactsByTagsRequest.toDomainRequest(): StreamFactsByTagsRequest = parseRequest {
    StreamFactsByTagsRequest(
        storeName = storeName.asStoreName(),
        tags = tagsMap.asTags(),
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcStreamFactsByTypeRequest = FactStoreProto.StreamFactsByTypeRequest

internal fun GrpcStreamFactsByTypeRequest.toDomainRequest(): StreamFactsByTypeRequest = parseRequest {
    StreamFactsByTypeRequest(
        storeName = storeName.asStoreName(),
        type = type.asFactType(),
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcStreamFactsBySubjectRequest = FactStoreProto.StreamFactsBySubjectRequest

internal fun GrpcStreamFactsBySubjectRequest.toDomainRequest(): StreamFactsBySubjectRequest = parseRequest {
    StreamFactsBySubjectRequest(
        storeName = storeName.asStoreName(),
        subject = subject.asSubject(),
        direction = direction.toCore(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
    )
}

typealias GrpcFindByTagsRequest = FactStoreProto.FindFactsByTagsRequest

internal fun GrpcFindByTagsRequest.toDomainRequest(): FindByTagsRequest = parseRequest {
    FindByTagsRequest(
        storeName = storeName.asStoreName(),
        tags = tagsMap.asTags(),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
        direction = direction.toCore(),
    )
}

typealias GrpcQueryFactsRequest = FactStoreProto.QueryFactsRequest

internal fun GrpcQueryFactsRequest.toDomainRequest(): FindByTagQueryRequest = parseRequest {
    FindByTagQueryRequest(
        storeName = storeName.asStoreName(),
        query = query.toDomain(),
    )
}

typealias GrpcFindInTimeRangeRequest = FactStoreProto.FindFactsInTimeRangeRequest

internal fun GrpcFindInTimeRangeRequest.toDomainRequest(): FindInTimeRangeRequest = parseRequest {
    FindInTimeRangeRequest(
        storeName = storeName.asStoreName(),
        timeRange = TimeRange(
            start = if (hasFrom()) from.toInstant() else null,
            end = if (hasTo()) to.toInstant() else null,
        ),
        limit = if (hasLimit()) Limit.of(limit) else Limit.None,
        direction = direction.toCore(),
    )
}

typealias GrpcSubscribeFactsRequest = FactStoreProto.SubscribeFactsRequest

internal fun GrpcSubscribeFactsRequest.toDomainRequest(): SubscribeRequest = parseRequest {
    val startPosition = when (startPositionCase) {
        FactStoreProto.SubscribeFactsRequest.StartPositionCase.FROM_END -> StartPosition.End
        FactStoreProto.SubscribeFactsRequest.StartPositionCase.AFTER_FACT_ID -> StartPosition.After(afterFactId.asFactId())
        else -> StartPosition.Beginning
    }
    SubscribeRequest(
        storeName = storeName.asStoreName(),
        startPosition = startPosition,
    )
}

typealias GrpcReplayFactsRequest = FactStoreProto.ReplayFactsRequest

internal fun GrpcReplayFactsRequest.toDomainRequest(): ReplayRequest = parseRequest {
    val start = when (startCase) {
        FactStoreProto.ReplayFactsRequest.StartCase.AFTER_FACT_ID -> ReplayStart.After(afterFactId.asFactId())
        else -> ReplayStart.Beginning
    }
    ReplayRequest(
        storeName = storeName.asStoreName(),
        start = start,
    )
}

typealias GrpcExistsStoreRequest = FactStoreProto.StoreExistsRequest

internal fun GrpcExistsStoreRequest.toDomainRequest(): ExistsStoreByNameRequest = parseRequest {
    ExistsStoreByNameRequest(name.asStoreName())
}
