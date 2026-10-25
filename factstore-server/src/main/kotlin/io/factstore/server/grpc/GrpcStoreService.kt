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
        factStore.createStore(request.toDomainRequest()).toGrpcResponse()

    override suspend fun getStore(request: FactStoreProto.GetStoreRequest): FactStoreProto.GetStoreResponse =
        factStore.findStoreByName(request.toDomainRequest()).toGrpcResponse()

    override suspend fun listStores(request: FactStoreProto.ListStoresRequest): FactStoreProto.ListStoresResponse =
        factStore.listStores().toGrpcResponse()

    override suspend fun deleteStore(request: FactStoreProto.DeleteStoreRequest): FactStoreProto.DeleteStoreResponse =
        factStore.removeStore(request.toDomainRequest()).toGrpcResponse()

    override suspend fun storeExists(request: FactStoreProto.StoreExistsRequest): FactStoreProto.StoreExistsResponse =
        factStore.existsStoreByName(request.toDomainRequest()).toGrpcResponse()
}
