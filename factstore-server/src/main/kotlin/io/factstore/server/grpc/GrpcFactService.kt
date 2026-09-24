package io.factstore.server.grpc

import io.factstore.core.*
import io.factstore.server.publishTo
import io.factstore.grpc.v1.FactServiceGrpcKt
import io.factstore.grpc.v1.FactStoreProto.*
import io.factstore.grpc.v1.continuationNotFound
import io.factstore.grpc.v1.factNotFound
import io.factstore.grpc.v1.storeNotFound
import io.factstore.grpc.v1.streamFactsBySubjectResponse
import io.factstore.grpc.v1.streamFactsByQueryResponse
import io.factstore.grpc.v1.streamFactsByTagsResponse
import io.factstore.grpc.v1.streamFactsByTypeResponse
import io.factstore.grpc.v1.streamFactsResponse
import io.factstore.grpc.v1.subscribeFactsResponse
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


    // Parsing and the store lookup must happen per call rather than when the Flow is built,
    // so the bodies of the streaming RPCs are wrapped in `flow { }` and only run on collection.

    override fun streamFacts(request: GrpcStreamFactsRequest): Flow<StreamFactsResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is StreamFactsResult.StoreNotFound -> flowOf(streamFactsResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is StreamFactsResult.ContinuationNotFound -> flowOf(streamFactsResponse {
                    continuationNotFound = continuationNotFound { factId = result.factId.uuid.toString() }
                })

                is StreamFactsResult.FactStream -> result.facts.toProtoFactBatches().map { facts ->
                    streamFactsResponse { batch = facts }
                }
            }
        )
    }

    override fun streamFactsBySubject(request: GrpcStreamFactsBySubjectRequest): Flow<StreamFactsBySubjectResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is StreamFactsBySubjectResult.StoreNotFound -> flowOf(streamFactsBySubjectResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is StreamFactsBySubjectResult.ContinuationNotFound -> flowOf(streamFactsBySubjectResponse {
                    continuationNotFound = continuationNotFound { factId = result.factId.uuid.toString() }
                })

                is StreamFactsBySubjectResult.FactStream -> result.facts.toProtoFactBatches().map { facts ->
                    streamFactsBySubjectResponse { batch = facts }
                }
            }
        )
    }

    override fun streamFactsByType(request: GrpcStreamFactsByTypeRequest): Flow<StreamFactsByTypeResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is StreamFactsByTypeResult.StoreNotFound -> flowOf(streamFactsByTypeResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is StreamFactsByTypeResult.ContinuationNotFound -> flowOf(streamFactsByTypeResponse {
                    continuationNotFound = continuationNotFound { factId = result.factId.uuid.toString() }
                })

                is StreamFactsByTypeResult.FactStream -> result.facts.toProtoFactBatches().map { facts ->
                    streamFactsByTypeResponse { batch = facts }
                }
            }
        )
    }

    override fun streamFactsByTags(request: GrpcStreamFactsByTagsRequest): Flow<StreamFactsByTagsResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is StreamFactsByTagsResult.StoreNotFound -> flowOf(streamFactsByTagsResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is StreamFactsByTagsResult.ContinuationNotFound -> flowOf(streamFactsByTagsResponse {
                    continuationNotFound = continuationNotFound { factId = result.factId.uuid.toString() }
                })

                is StreamFactsByTagsResult.FactStream -> result.facts.toProtoFactBatches().map { facts ->
                    streamFactsByTagsResponse { batch = facts }
                }
            }
        )
    }

    override fun streamFactsByQuery(request: GrpcStreamFactsByQueryRequest): Flow<StreamFactsByQueryResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is StreamFactsByQueryResult.StoreNotFound -> flowOf(streamFactsByQueryResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is StreamFactsByQueryResult.ContinuationNotFound -> flowOf(streamFactsByQueryResponse {
                    continuationNotFound = continuationNotFound { factId = result.factId.uuid.toString() }
                })

                is StreamFactsByQueryResult.FactStream -> result.facts.toProtoFactBatches().map { facts ->
                    streamFactsByQueryResponse { batch = facts }
                }
            }
        )
    }

    override fun subscribeFacts(request: SubscribeFactsRequest): Flow<SubscribeFactsResponse> = flow {
        emitAll(
            when (val result = request.toDomainRequest().publishTo(factStore)) {
                is SubscribeResult.StoreNotFound -> flowOf(subscribeFactsResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is SubscribeResult.FactIdNotFound -> flowOf(subscribeFactsResponse {
                    afterFactNotFound = factNotFound { }
                })

                is SubscribeResult.FactStream -> result.stream.map { facts ->
                    subscribeFactsResponse { batch = facts.toProtoFactBatch() }
                }
            }
        )
    }

}
