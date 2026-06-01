package io.factstore.server.grpc

import io.factstore.core.*
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.quarkus.grpc.GrpcService
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import java.time.Instant
import java.util.*

@GrpcService
class GrpcFactService(
    private val factStore: FactStore,
    vertx: Vertx,
) : FactService, CoroutineScope by CoroutineScope(SupervisorJob() + vertx.dispatcher()) {

    override fun appendFacts(request: FactStoreProto.AppendFactsRequest): Uni<FactStoreProto.AppendFactsResponse> =
        toUni {
            val facts = request.factsList.map { it.toDomain() }
            val idempotencyKey = if (request.hasIdempotencyKey())
                IdempotencyKey(UUID.fromString(request.idempotencyKey))
            else
                IdempotencyKey()
            val condition = if (request.hasCondition()) request.condition.toDomain() else AppendCondition.None

            val result = factStore.append(
                AppendRequest(
                    storeName = StoreName(request.storeName),
                    facts = facts,
                    idempotencyKey = idempotencyKey,
                    condition = condition
                )
            )

            appendFactsResponse {
                when (result) {
                    is AppendResult.Appended -> appended = factsAppended { }
                    is AppendResult.AlreadyApplied -> alreadyApplied = alreadyApplied { }
                    is AppendResult.AppendConditionViolated -> conditionViolated = conditionViolated { }
                    is AppendResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = result.storeName.value }
                    is AppendResult.DuplicateFactIds -> duplicateFactIds = duplicateFactIds {
                        factIds += result.factIds.map { it.uuid.toString() }
                    }
                }
            }
        }

    override fun getFact(request: FactStoreProto.GetFactRequest): Uni<FactStoreProto.GetFactResponse> =
        toUni {
            val result = factStore.findById(
                storeName = StoreName(request.storeName),
                factId = UUID.fromString(request.factId).toFactId()
            )
            getFactResponse {
                when (result) {
                    is FindByIdResult.Found -> found = factFound { fact = result.fact.toProto() }
                    is FindByIdResult.NotFound -> notFound = factNotFound { }
                    is FindByIdResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = result.storeName.value }
                }
            }
        }

    override fun factExists(request: FactStoreProto.FactExistsRequest): Uni<FactStoreProto.FactExistsResponse> =
        toUni {
            val result = factStore.existsById(
                storeName = StoreName(request.storeName),
                factId = UUID.fromString(request.factId).toFactId()
            )
            factExistsResponse {
                when (result) {
                    ExistsByIdResult.Exists -> present = factPresent { }
                    ExistsByIdResult.DoesNotExist -> absent = factAbsent {  }
                    ExistsByIdResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = request.storeName }
                }
            }
        }

    override fun findFactsBySubject(request: FactStoreProto.FindFactsBySubjectRequest): Uni<FactStoreProto.FindFactsBySubjectResponse> =
        toUni {
            val result = factStore.findBySubject(
                storeName = StoreName(request.storeName),
                subject = Subject(request.subject),
                limit = request.limit.toLimit(),
                direction = request.direction.toCore()
            )
            findFactsBySubjectResponse {
                when (result) {
                    is FindBySubjectResult.Found ->
                        found = factsFound { facts += result.facts.map { it.toProto() } }
                    is FindBySubjectResult.StoreNotFound ->
                        storeNotFound = storeNotFound { storeName = result.storeName.value }
                }
            }
        }

    override fun findFactsByTags(request: FactStoreProto.FindFactsByTagsRequest): Uni<FactStoreProto.FindFactsByTagsResponse> =
        toUni {
            val tags = request.tagsMap.entries.map { (k, v) -> k.toTagKey() to v.toTagValue() }
            val result = factStore.findByTags(
                storeName = StoreName(request.storeName),
                tags = tags,
                limit = request.limit.toLimit(),
                direction = request.direction.toCore()
            )
            findFactsByTagsResponse {
                when (result) {
                    is FindByTagsResult.Found ->
                        found = factsFound { facts += result.facts.map { it.toProto() } }
                    is FindByTagsResult.StoreNotFound ->
                        storeNotFound = storeNotFound { storeName = result.storeName.value }
                }
            }
        }

    override fun queryFacts(request: FactStoreProto.QueryFactsRequest): Uni<FactStoreProto.QueryFactsResponse> =
        toUni {
            val result = factStore.findByTagQuery(
                storeName = StoreName(request.storeName),
                query = request.query.toDomain()
            )
            queryFactsResponse {
                when (result) {
                    is FindByTagQueryResult.Found ->
                        found = factsFound { facts += result.facts.map { it.toProto() } }
                    is FindByTagQueryResult.StoreNotFound ->
                        storeNotFound = storeNotFound { storeName = result.storeName.value }
                }
            }
        }

    override fun findFactsInTimeRange(request: FactStoreProto.FindFactsInTimeRangeRequest): Uni<FactStoreProto.FindFactsInTimeRangeResponse> =
        toUni {
            val result = factStore.findInTimeRange(
                storeName = StoreName(request.storeName),
                timeRange = TimeRange(
                    start = if (request.hasFrom()) request.from.toInstant() else Instant.MIN,
                    end = if (request.hasTo()) request.to.toInstant() else Instant.now()
                ),
                limit = request.limit.toLimit(),
                direction = request.direction.toCore()
            )
            findFactsInTimeRangeResponse {
                when (result) {
                    is FindInTimeRangeResult.Found ->
                        found = factsFound { facts += result.facts.map { it.toProto() } }
                    is FindInTimeRangeResult.StoreNotFound ->
                        storeNotFound = storeNotFound { storeName = result.storeName.value }
                }
            }
        }

    override fun streamFacts(
        request: FactStoreProto.StreamFactsRequest
    ): Multi<FactStoreProto.Fact> = toMulti {
        val startPosition = when (request.startPositionCase) {
            FactStoreProto.StreamFactsRequest.StartPositionCase.FROM_END -> StartPosition.End
            FactStoreProto.StreamFactsRequest.StartPositionCase.AFTER_FACT_ID -> StartPosition.After(
                UUID.fromString(request.afterFactId).toFactId()
            )
            else -> StartPosition.Beginning
        }

        flow {
            when (val result = factStore.stream(
                StoreName(request.storeName),
                StreamingOptions(startPosition)
            )) {
                is StreamResult.StoreNotFound -> throw StatusRuntimeException(
                    Status.NOT_FOUND.withDescription("Store '${result.storeName.value}' not found")
                )
                is StreamResult.FactIdNotFound -> throw StatusRuntimeException(
                    Status.NOT_FOUND.withDescription("Fact '${result.id.uuid}' not found")
                )
                is StreamResult.FactStream -> emitAll(result.stream.map { it.toProto() })
            }
        }
    }
}
