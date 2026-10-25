package io.factstore.server.grpc

import io.factstore.core.*
import io.factstore.grpc.v1.FactServiceGrpcKt
import io.factstore.grpc.v1.FactStoreProto.*
import io.factstore.grpc.v1.continuationNotFound
import io.factstore.grpc.v1.storeNotFound
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
        factStore.append(request.toDomainRequest()).toGrpcResponse()

    override suspend fun getFact(request: GetFactRequest): GetFactResponse =
        factStore.findById(request.toDomainRequest()).toGrpcResponse()

    override suspend fun factExists(request: FactExistsRequest): FactExistsResponse =
        factStore.existsById(request.toDomainRequest()).toGrpcResponse()


    // Parsing and the store lookup must happen per call rather than when the Flow is built,
    // so the bodies of the streaming RPCs are wrapped in `flow { }` and only run on collection.

    override fun streamFacts(request: GrpcStreamFactsRequest): Flow<StreamFactsResponse> = flow {
        emitAll(
            when (val result = factStore.streamFacts(request.toDomainRequest())) {
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

    override fun subscribeFacts(request: SubscribeFactsRequest): Flow<SubscribeFactsResponse> = flow {
        emitAll(
            when (val result = factStore.subscribe(request.toDomainRequest())) {
                is SubscribeResult.StoreNotFound -> flowOf(subscribeFactsResponse {
                    storeNotFound = storeNotFound { storeName = result.storeName.value }
                })

                is SubscribeResult.ContinuationNotFound -> flowOf(subscribeFactsResponse {
                    continuationNotFound = continuationNotFound { factId = result.factId.uuid.toString() }
                })

                is SubscribeResult.FactStream -> result.facts.toProtoFactBatches().map { batch ->
                    subscribeFactsResponse { this.batch = batch }
                }
            }
        )
    }

}
