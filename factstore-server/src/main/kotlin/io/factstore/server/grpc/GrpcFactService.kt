package io.factstore.server.grpc

import io.factstore.core.*
import io.factstore.grpc.v1.FactService
import io.factstore.grpc.v1.FactStoreProto
import io.factstore.grpc.v1.FactStoreProto.*
import io.factstore.grpc.v1.factNotFound
import io.factstore.grpc.v1.storeNotFound
import io.factstore.grpc.v1.streamFactsResponse
import io.quarkus.grpc.GrpcService
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map

@GrpcService
class GrpcFactService(
    private val factStore: FactStore,
    vertx: Vertx,
) : FactService {

    private val grpcContext = vertx.dispatcher()

    override fun appendFacts(
        request: AppendFactsRequest
    ): Uni<AppendFactsResponse> = toUni(grpcContext) {
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
    }

    override fun getFact(
        request: GetFactRequest
    ): Uni<GetFactResponse> = toUni(grpcContext) {
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
    }

    override fun factExists(
        request: FactExistsRequest
    ): Uni<FactExistsResponse> = toUni(grpcContext) {
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
    }

    override fun findFactsBySubject(
        request: FindFactsBySubjectRequest
    ): Uni<FindFactsBySubjectResponse> = toUni(grpcContext) {
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
    }

    override fun findFactsByTags(
        request: FindFactsByTagsRequest
    ): Uni<FindFactsByTagsResponse> = toUni(grpcContext) {
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
    }

    override fun queryFacts(
        request: QueryFactsRequest
    ): Uni<QueryFactsResponse> = toUni(grpcContext) {
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
    }

    override fun findFactsInTimeRange(
        request: FindFactsInTimeRangeRequest
    ): Uni<FindFactsInTimeRangeResponse> = toUni(grpcContext) {
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
    }

    override fun streamFacts(
        request: FactStoreProto.StreamFactsRequest
    ): Multi<StreamFactsResponse> = toMulti(grpcContext) {
        when (val result = request.toDomainRequest().publishTo(factStore)) {
            is StreamResult.StoreNotFound -> flowOf(streamFactsResponse {
                storeNotFound = storeNotFound { storeName = result.storeName.value }
            })
            is StreamResult.FactIdNotFound -> flowOf(streamFactsResponse {
                afterFactNotFound = factNotFound { }
            })
            is StreamResult.FactStream -> result.stream.map { facts ->
                streamFactsResponse { batch = facts.toProtoFactBatch() }
            }
        }
    }
}
