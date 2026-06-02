package io.factstore.server.grpc

import io.factstore.core.*
import io.quarkus.grpc.GrpcService
import io.smallrye.mutiny.Uni
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.dispatcher

@GrpcService
class GrpcStoreService(
    private val factStore: FactStore,
    vertx: Vertx,
) : StoreService {

    private val grpcContext = vertx.dispatcher()

    override fun createStore(request: FactStoreProto.CreateStoreRequest): Uni<FactStoreProto.CreateStoreResponse> =
        toUni(grpcContext) {
            request.toDomainRequest().publishTo(factStore).toGrpcResponse()
        }

    override fun getStore(request: FactStoreProto.GetStoreRequest): Uni<FactStoreProto.GetStoreResponse> =
        toUni(grpcContext) {
            request.toDomainRequest().publishTo(factStore).toGrpcResponse()
        }

    override fun listStores(request: FactStoreProto.ListStoresRequest): Uni<FactStoreProto.ListStoresResponse> =
        toUni(grpcContext) {
            factStore.listAll().toGrpcResponse()
        }

    override fun deleteStore(request: FactStoreProto.DeleteStoreRequest): Uni<FactStoreProto.DeleteStoreResponse> =
        toUni(grpcContext) {
            request.toDomainRequest().publishTo(factStore).toGrpcResponse()
        }

    override fun storeExists(request: FactStoreProto.StoreExistsRequest): Uni<FactStoreProto.StoreExistsResponse> =
        toUni(grpcContext) {
            request.toDomainRequest().publishTo(factStore).toGrpcResponse()

        }
}
