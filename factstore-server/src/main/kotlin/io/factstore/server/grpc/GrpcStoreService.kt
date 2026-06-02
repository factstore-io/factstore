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
            val result = factStore.handle(CreateStoreRequest(StoreName(request.name)))
            createStoreResponse {
                when (result) {
                    is CreateStoreResult.Created ->
                        created = storeCreated { id = result.id.uuid.toString() }
                    is CreateStoreResult.NameAlreadyExists ->
                        nameAlreadyExists = storeNameAlreadyExists { }
                }
            }
        }

    override fun getStore(request: FactStoreProto.GetStoreRequest): Uni<FactStoreProto.GetStoreResponse> =
        toUni(grpcContext) {
            val storeMetadata = factStore.findByName(StoreName(request.name))
            getStoreResponse {
                if (storeMetadata != null) {
                    found = storeFound { store = storeMetadata.toProto() }
                } else {
                    notFound = storeNotFound { storeName = request.name }
                }
            }
        }

    override fun listStores(request: FactStoreProto.ListStoresRequest): Uni<FactStoreProto.ListStoresResponse> =
        toUni(grpcContext) {
            val all = factStore.listAll()
            listStoresResponse {
                stores += all.map { it.toProto() }
            }
        }

    override fun deleteStore(request: FactStoreProto.DeleteStoreRequest): Uni<FactStoreProto.DeleteStoreResponse> =
        toUni(grpcContext) {
            val result = factStore.handle(RemoveStoreRequest(StoreName(request.name)))
            deleteStoreResponse {
                when (result) {
                    is RemoveStoreResult.StoreRemoved -> deleted = storeDeleted { }
                    is RemoveStoreResult.StoreNotFound -> notFound = storeNotFound { storeName = result.storeName.value }
                }
            }
        }

    override fun storeExists(request: FactStoreProto.StoreExistsRequest): Uni<FactStoreProto.StoreExistsResponse> =
        toUni(grpcContext) {
            val found = factStore.existsByName(StoreName(request.name))
            storeExistsResponse {
                when (found) {
                    true -> present = storePresent {}
                    false -> absent = storeAbsent {}
                }
            }
        }
}
