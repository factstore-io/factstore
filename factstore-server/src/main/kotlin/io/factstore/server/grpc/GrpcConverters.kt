package io.factstore.server.grpc

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.factstore.core.*
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.future
import kotlinx.coroutines.jdk9.asPublisher
import java.time.Instant
import java.util.*
import kotlin.collections.map
import kotlin.coroutines.CoroutineContext
import io.factstore.core.ReadDirection as CoreReadDirection


/**
 * Bridges a suspending block to a Mutiny [Uni].
 *
 * Exists because Quarkus generates gRPC service stubs against Mutiny rather than
 * Kotlin coroutines. We use this at the gRPC adapter boundary to keep service bodies
 * written as ordinary suspend functions.
 *
 * @param context Coroutine context for this call.
 * @param block Suspending body to execute on subscription.
 * @return A cold [Uni] that emits the result of [block] or fails with its exception.
 */
internal fun <T> toUni(
    context: CoroutineContext,
    block: suspend () -> T
): Uni<T> =
    Uni.createFrom().completionStage {
        CoroutineScope(context).future { block() }
    }

/**
 * Bridges a suspending [Flow] producer to a Mutiny [Multi].
 *
 * Exists because Quarkus generates gRPC server-streaming stubs against Mutiny
 * rather than Kotlin coroutines. We use this at the gRPC adapter boundary to keep
 * streaming bodies written as ordinary [Flow]s.
 *
 * @param context Coroutine context for this call.
 * @param block Suspending producer of the [Flow] to stream. Invoked once per
 *   subscription, so any per-call setup belongs inside it.
 * @return A cold [Multi] that mirrors the [Flow] returned by [block].
 */
internal fun <T : Any> toMulti(
    context: CoroutineContext,
    block: suspend () -> Flow<T>,
): Multi<T> =
    Multi.createFrom().publisher(
        flow { emitAll(block()) }.asPublisher(context)
    )

internal fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder()
    .setSeconds(epochSecond)
    .setNanos(nano)
    .build()

internal fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

internal fun Int.toLimit(): Limit = if (this > 0) Limit.of(this) else Limit.None

internal fun FactStoreProto.ReadDirection.toCore(): CoreReadDirection = when (this) {
    FactStoreProto.ReadDirection.BACKWARD -> CoreReadDirection.Backward
    else -> CoreReadDirection.Forward
}

internal fun Fact.toProto(): FactStoreProto.Fact = fact {
    id = this@toProto.id.uuid.toString()
    type = this@toProto.type.value
    subject = this@toProto.subject.value
    appendedAt = this@toProto.appendedAt.toTimestamp()
    payload = this@toProto.payload.toProto()
    metadata.putAll(this@toProto.metadata)
    tags.putAll(this@toProto.tags.entries.associate { (k, v) -> k.value to v.value })
}

internal fun FactPayload.toProto(): FactStoreProto.FactPayload = factPayload {
    data = ByteString.copyFrom(this@toProto.data)
    this@toProto.format?.let { format = it.value }
    this@toProto.schema?.let { schemaRef = it.value }
}

internal fun StoreMetadata.toProto(): FactStoreProto.StoreInfo = storeInfo {
    id = this@toProto.id.uuid.toString()
    name = this@toProto.name.value
    createdAt = this@toProto.createdAt.toTimestamp()
}

internal fun FactStoreProto.FactInput.toDomain(): Fact = Fact(
    id = if (hasId()) UUID.fromString(id).toFactId() else FactId.generate(),
    type = type.toFactType(),
    subject = Subject(subject),
    appendedAt = if (hasAppendedAt()) appendedAt.toInstant() else Instant.now(),
    payload = payload.toDomain(),
    metadata = metadataMap,
    tags = tagsMap.entries.associate { (k, v) -> k.toTagKey() to v.toTagValue() }
)

internal fun FactStoreProto.FactPayload.toDomain(): FactPayload = FactPayload(
    data = data.toByteArray(),
    format = if (hasFormat()) PayloadFormat(format) else null,
    schema = if (hasSchemaRef()) PayloadSchemaRef(schemaRef) else null
)

internal fun FactStoreProto.AppendCondition.toDomain(): AppendCondition = when (kindCase) {
    FactStoreProto.AppendCondition.KindCase.EXPECTED_LAST_FACT -> AppendCondition.ExpectedLastFact(
        subject = Subject(expectedLastFact.subject),
        expectedLastFactId = if (expectedLastFact.hasExpectedLastFactId())
            UUID.fromString(expectedLastFact.expectedLastFactId).toFactId()
        else null
    )

    FactStoreProto.AppendCondition.KindCase.EXPECTED_MULTI_SUBJECT_LAST_FACT -> AppendCondition.ExpectedMultiSubjectLastFact(
        expectations = expectedMultiSubjectLastFact.expectationsList.associate { exp ->
            Subject(exp.subject) to
                    if (exp.hasExpectedLastFactId()) UUID.fromString(exp.expectedLastFactId).toFactId()
                    else null
        }
    )

    FactStoreProto.AppendCondition.KindCase.TAG_QUERY_BASED -> AppendCondition.TagQueryBased(
        failIfEventsMatch = tagQueryBased.failIfEventsMatch.toDomain(),
        after = if (tagQueryBased.hasAfterFactId()) UUID.fromString(tagQueryBased.afterFactId).toFactId()
        else null
    )

    else -> AppendCondition.None
}

internal fun FactStoreProto.TagQuery.toDomain(): TagQuery = TagQuery(
    queryItems = itemsList.map { it.toDomain() }
)

internal fun FactStoreProto.TagQueryItem.toDomain(): TagQueryItem = when (kindCase) {
    FactStoreProto.TagQueryItem.KindCase.TAG_ONLY -> TagOnlyQueryItem(
        tags = tagOnly.tagsMap.entries.map { (k, v) -> k.toTagKey() to v.toTagValue() }
    )

    FactStoreProto.TagQueryItem.KindCase.TAG_TYPE -> TagTypeItem(
        types = tagType.typesList.map { it.toFactType() },
        tags = tagType.tagsMap.entries.map { (k, v) -> k.toTagKey() to v.toTagValue() }
    )

    else -> throw IllegalArgumentException("TagQueryItem has no kind set")
}

typealias GrpcAppendRequest = FactStoreProto.AppendFactsRequest
typealias GrpcAppendFactsResponse = FactStoreProto.AppendFactsResponse

internal fun GrpcAppendRequest.toDomainRequest(): AppendRequest {
    val storeName = StoreName(storeName)
    val facts = factsList.map { it.toDomain() }
    val idempotencyKey = if (hasIdempotencyKey())
        IdempotencyKey(UUID.fromString(idempotencyKey))
    else
        IdempotencyKey()
    val condition = if (hasCondition()) condition.toDomain() else AppendCondition.None

    return AppendRequest(
        storeName = storeName,
        facts = facts,
        idempotencyKey = idempotencyKey,
        condition = condition
    )
}

internal suspend fun AppendRequest.publishTo(factStore: FactStore): AppendResult =
    factStore.append(this)

internal fun AppendResult.toGrpcResponse(): GrpcAppendFactsResponse =
    appendFactsResponse {
        when (this@toGrpcResponse) {
            is AppendResult.Appended -> appended = factsAppended { }
            is AppendResult.AlreadyApplied -> alreadyApplied = alreadyApplied { }
            is AppendResult.AppendConditionViolated -> conditionViolated = conditionViolated { }
            is AppendResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
            is AppendResult.DuplicateFactIds -> duplicateFactIds = duplicateFactIds {
                factIds += this@toGrpcResponse.factIds.map { it.uuid.toString() }
            }
        }
    }

typealias GrpcGetFactResponse = FactStoreProto.GetFactResponse

internal fun FindByIdResult.toGrpcResponse(): GrpcGetFactResponse =
    getFactResponse {
        when (this@toGrpcResponse) {
            is FindByIdResult.Found -> found = factFound { fact = this@toGrpcResponse.fact.toProto() }
            is FindByIdResult.NotFound -> notFound = factNotFound { }
            is FindByIdResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

internal fun ExistsByIdResult.toGrpcResponse(): FactStoreProto.FactExistsResponse =
    factExistsResponse {
        when (this@toGrpcResponse) {
            ExistsByIdResult.Exists -> present = factPresent { }
            ExistsByIdResult.DoesNotExist -> absent = factAbsent { }
            is ExistsByIdResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }


typealias GrpcFindBySubjectResponse = FactStoreProto.FindFactsBySubjectResponse

internal fun FindBySubjectResult.toGrpcResponse(): GrpcFindBySubjectResponse =
    findFactsBySubjectResponse {
        when (this@toGrpcResponse) {
            is FindBySubjectResult.Found ->
                found = factsFound { facts += this@toGrpcResponse.facts.map { it.toProto() } }

            is FindBySubjectResult.StoreNotFound ->
                storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcFindFactsByTagsResponse = FactStoreProto.FindFactsByTagsResponse

internal fun FindByTagsResult.toGrpcResponse(): GrpcFindFactsByTagsResponse =
    findFactsByTagsResponse {
        when (this@toGrpcResponse) {
            is FindByTagsResult.Found ->
                found = factsFound { facts += this@toGrpcResponse.facts.map { it.toProto() } }

            is FindByTagsResult.StoreNotFound ->
                storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

internal fun FindByTagQueryResult.toGrpcResponse(): FactStoreProto.QueryFactsResponse =
    queryFactsResponse {
        when (this@toGrpcResponse) {
            is FindByTagQueryResult.Found ->
                found = factsFound { facts += this@toGrpcResponse.facts.map { it.toProto() } }

            is FindByTagQueryResult.StoreNotFound ->
                storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }


internal fun FindInTimeRangeResult.toGrpcResponse(): FactStoreProto.FindFactsInTimeRangeResponse =
    findFactsInTimeRangeResponse {
        when (this@toGrpcResponse) {
            is FindInTimeRangeResult.Found ->
                found = factsFound { facts += this@toGrpcResponse.facts.map { it.toProto() } }

            is FindInTimeRangeResult.StoreNotFound ->
                storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcCreateStoreRequest = FactStoreProto.CreateStoreRequest
typealias GrpcCreateStoreResponse = FactStoreProto.CreateStoreResponse

internal fun GrpcCreateStoreRequest.toDomainRequest(): CreateStoreRequest =
    CreateStoreRequest(StoreName(name))

internal suspend fun CreateStoreRequest.publishTo(factStore: FactStore): CreateStoreResult =
    factStore.handle(this)

internal fun CreateStoreResult.toGrpcResponse(): GrpcCreateStoreResponse =
    createStoreResponse {
        when (this@toGrpcResponse) {
            is CreateStoreResult.Created ->
                created = storeCreated { id = this@toGrpcResponse.id.uuid.toString() }
            is CreateStoreResult.NameAlreadyExists ->
                nameAlreadyExists = storeNameAlreadyExists { }
        }
    }

internal fun List<StoreMetadata>.toGrpcResponse(): FactStoreProto.ListStoresResponse =
    listStoresResponse {
        stores += this@toGrpcResponse.map { it.toProto() }
    }

typealias GrpcDeleteStoreRequest = FactStoreProto.DeleteStoreRequest
typealias GrpcDeleteStoreResponse = FactStoreProto.DeleteStoreResponse

internal fun GrpcDeleteStoreRequest.toDomainRequest(): RemoveStoreRequest =
    RemoveStoreRequest(StoreName(name))

internal suspend fun RemoveStoreRequest.publishTo(factStore: FactStore): RemoveStoreResult =
    factStore.handle(this)

internal fun RemoveStoreResult.toGrpcResponse(): GrpcDeleteStoreResponse =
    deleteStoreResponse {
        when (this@toGrpcResponse) {
            is RemoveStoreResult.StoreRemoved -> deleted = storeDeleted { }
            is RemoveStoreResult.StoreNotFound -> notFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcFindStoreByNameRequest = FactStoreProto.GetStoreRequest
typealias GrpcFindStoreByNameResult = FactStoreProto.GetStoreResponse

internal fun GrpcFindStoreByNameRequest.toDomainRequest(): FindStoreByNameRequest =
    FindStoreByNameRequest(StoreName(name))

internal suspend fun FindStoreByNameRequest.publishTo(factStore: FactStore): FindStoreByNameResult =
    factStore.findByName(this)

internal fun FindStoreByNameResult.toGrpcResponse(): GrpcFindStoreByNameResult =
    when (this) {
        is FindStoreByNameResult.Found -> getStoreResponse { found = storeFound { store = storeMetadata.toProto() } }
        is FindStoreByNameResult.NotFound -> getStoreResponse { notFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value } }
    }


typealias GrpcGetFactRequest = FactStoreProto.GetFactRequest

internal fun GrpcGetFactRequest.toDomainRequest(): FindByIdRequest =
    FindByIdRequest(
        storeName = StoreName(storeName),
        factId = UUID.fromString(factId).toFactId()
    )

internal suspend fun FindByIdRequest.publishTo(factStore: FactStore): FindByIdResult =
    factStore.findById(this)

typealias GrpcFactExistsRequest = FactStoreProto.FactExistsRequest

internal fun GrpcFactExistsRequest.toDomainRequest(): ExistsByIdRequest =
    ExistsByIdRequest(
        storeName = StoreName(storeName),
        factId = UUID.fromString(factId).toFactId()
    )

internal suspend fun ExistsByIdRequest.publishTo(factStore: FactStore): ExistsByIdResult =
    factStore.existsById(this)

typealias GrpcFindBySubjectRequest = FactStoreProto.FindFactsBySubjectRequest

internal fun GrpcFindBySubjectRequest.toDomainRequest(): FindBySubjectRequest =
    FindBySubjectRequest(
        storeName = StoreName(storeName),
        subject = Subject(subject),
        limit = limit.toLimit(),
        direction = direction.toCore()
    )

internal suspend fun FindBySubjectRequest.publishTo(factStore: FactStore): FindBySubjectResult =
    factStore.findBySubject(this)

typealias GrpcFindByTagsRequest = FactStoreProto.FindFactsByTagsRequest

internal fun GrpcFindByTagsRequest.toDomainRequest(): FindByTagsRequest =
    FindByTagsRequest(
        storeName = StoreName(storeName),
        tags = tagsMap.entries.map { (k, v) -> k.toTagKey() to v.toTagValue() },
        limit = limit.toLimit(),
        direction = direction.toCore()
    )

internal suspend fun FindByTagsRequest.publishTo(factStore: FactStore): FindByTagsResult =
    factStore.findByTags(this)

typealias GrpcQueryFactsRequest = FactStoreProto.QueryFactsRequest

internal fun GrpcQueryFactsRequest.toDomainRequest(): FindByTagQueryRequest =
    FindByTagQueryRequest(
        storeName = StoreName(storeName),
        query = query.toDomain()
    )

internal suspend fun FindByTagQueryRequest.publishTo(factStore: FactStore): FindByTagQueryResult =
    factStore.findByTagQuery(this)

typealias GrpcFindInTimeRangeRequest = FactStoreProto.FindFactsInTimeRangeRequest

internal fun GrpcFindInTimeRangeRequest.toDomainRequest(): FindInTimeRangeRequest =
    FindInTimeRangeRequest(
        storeName = StoreName(storeName),
        timeRange = TimeRange(
            start = if (hasFrom()) from.toInstant() else Instant.MIN,
            end = if (hasTo()) to.toInstant() else Instant.MAX
        ),
        limit = limit.toLimit(),
        direction = direction.toCore()
    )

internal suspend fun FindInTimeRangeRequest.publishTo(factStore: FactStore): FindInTimeRangeResult =
    factStore.findInTimeRange(this)

typealias GrpcExistsStoreRequest = FactStoreProto.StoreExistsRequest

internal fun GrpcExistsStoreRequest.toDomainRequest(): ExistsStoreByNameRequest =
    ExistsStoreByNameRequest(StoreName(name))

internal suspend fun ExistsStoreByNameRequest.publishTo(factStore: FactStore): ExistsStoreByNameResult =
    factStore.existsByName(this)

typealias GrpcStoreExistsResponse = FactStoreProto.StoreExistsResponse

internal fun ExistsStoreByNameResult.toGrpcResponse(): GrpcStoreExistsResponse =
    when (this) {
        ExistsStoreByNameResult.StoreExists -> storeExistsResponse { present = storePresent { } }
        ExistsStoreByNameResult.StoreAbsent -> storeExistsResponse { absent = storeAbsent { } }
    }
