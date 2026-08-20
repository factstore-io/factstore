package io.factstore.server.grpc

import io.factstore.core.*
import io.factstore.grpc.v1.FactServiceGrpcKt
import io.factstore.grpc.v1.FactStoreProto.*
import io.factstore.grpc.v1.factNotFound
import io.factstore.grpc.v1.storeNotFound
import io.factstore.grpc.v1.streamFactsResponse
import io.quarkus.grpc.GrpcService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map

@GrpcService
class GrpcFactService(
    private val factStore: FactStore,
) : FactServiceGrpcKt.FactServiceCoroutineImplBase() {

    override suspend fun appendFacts(request: AppendFactsRequest): AppendFactsResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun getFact(request: GetFactRequest): GetFactResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun factExists(request: FactExistsRequest): FactExistsResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun findFactsBySubject(request: FindFactsBySubjectRequest): FindFactsBySubjectResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun findFactsByTags(request: FindFactsByTagsRequest): FindFactsByTagsResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun queryFacts(request: QueryFactsRequest): QueryFactsResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun findFactsInTimeRange(request: FindFactsInTimeRangeRequest): FindFactsInTimeRangeResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    // The store lookup must happen per subscription rather than when the Flow is built,
    // so the body is wrapped in `flow { }` and only runs on collection.
    override fun subscribeFacts(request: SubscribeFactsRequest): Flow<StreamFactsResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is SubscribeResult.StoreNotFound -> flowOf(streamFactsResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is SubscribeResult.FactIdNotFound -> flowOf(streamFactsResponse {
                    afterFactNotFound = factNotFound { }
                })

                is SubscribeResult.FactStream -> result.stream.map { facts ->
                    streamFactsResponse { batch = facts.toProtoFactBatch() }
                }
            }
        )
    }

    override fun replayFacts(request: ReplayFactsRequest): Flow<StreamFactsResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is ReplayResult.StoreNotFound -> flowOf(streamFactsResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is ReplayResult.FactIdNotFound -> flowOf(streamFactsResponse {
                    afterFactNotFound = factNotFound { }
                })

                is ReplayResult.FactStream -> result.stream.map { facts ->
                    streamFactsResponse { batch = facts.toProtoFactBatch() }
                }
            }
        )
    }
}
