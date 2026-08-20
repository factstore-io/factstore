package io.factstore.server.grpc

import io.factstore.core.*
import io.factstore.grpc.v1.FactStoreProto
import io.factstore.grpc.v1.StoreServiceGrpcKt
import io.quarkus.grpc.GrpcService

@GrpcService
class GrpcStoreService(
    private val factStore: FactStore,
) : StoreServiceGrpcKt.StoreServiceCoroutineImplBase() {

    override suspend fun createStore(request: FactStoreProto.CreateStoreRequest): FactStoreProto.CreateStoreResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun getStore(request: FactStoreProto.GetStoreRequest): FactStoreProto.GetStoreResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun listStores(request: FactStoreProto.ListStoresRequest): FactStoreProto.ListStoresResponse =
        factStore.listAll().toGrpcResponse()

    override suspend fun deleteStore(request: FactStoreProto.DeleteStoreRequest): FactStoreProto.DeleteStoreResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()

    override suspend fun storeExists(request: FactStoreProto.StoreExistsRequest): FactStoreProto.StoreExistsResponse =
        request.toDomainRequest().publishTo(factStore).toGrpcResponse()
}
