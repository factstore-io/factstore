package io.factstore.server.grpc

import io.factstore.core.*
import io.factstore.grpc.v1.FactService
import io.factstore.grpc.v1.FactStoreProto
import io.factstore.grpc.v1.FactStoreProto.*
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.quarkus.grpc.GrpcService
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.dispatcher
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
    ): Multi<FactBatch> = toMulti(grpcContext) {
        when (val result = request.toDomainRequest().publishTo(factStore)) {
            is StreamResult.StoreNotFound -> throw StatusRuntimeException(
                Status.FAILED_PRECONDITION.withDescription("Store '${result.storeName.value}' not found - create it first")
            )
            is StreamResult.FactIdNotFound -> throw StatusRuntimeException(
                Status.FAILED_PRECONDITION.withDescription("Fact '${result.id.uuid}' not found - cannot use it as a stream cursor")
            )
            is StreamResult.FactStream -> result.stream.map { it.toProtoFactBatch() }
        }
    }
}
