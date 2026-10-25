package io.factstore.client.operations

import io.factstore.client.exceptions.StoreNameAlreadyExistsException
import io.factstore.client.exceptions.StoreNotFoundException
import io.factstore.client.internal.grpcCall
import io.factstore.client.internal.toModel
import io.factstore.client.internal.withCallTimeout
import io.factstore.client.model.StoreInfo
import io.factstore.grpc.v1.FactStoreProto.CreateStoreResponse
import io.factstore.grpc.v1.FactStoreProto.DeleteStoreResponse
import io.factstore.grpc.v1.FactStoreProto.GetStoreResponse
import io.factstore.grpc.v1.FactStoreProto.StoreExistsResponse
import io.factstore.grpc.v1.StoreServiceGrpcKt.StoreServiceCoroutineStub
import io.factstore.grpc.v1.createStoreRequest
import io.factstore.grpc.v1.deleteStoreRequest
import io.factstore.grpc.v1.getStoreRequest
import io.factstore.grpc.v1.listStoresRequest
import io.factstore.grpc.v1.storeExistsRequest
import kotlin.time.Duration

class StoreOperations internal constructor(
    private val stub: StoreServiceCoroutineStub,
    private val callTimeout: Duration,
) {

    suspend fun create(name: String): String = grpcCall {
        val response = stub.withCallTimeout(callTimeout).createStore(createStoreRequest { this.name = name })
        when (response.outcomeCase) {
            CreateStoreResponse.OutcomeCase.CREATED -> response.created.id
            CreateStoreResponse.OutcomeCase.NAME_ALREADY_EXISTS -> throw StoreNameAlreadyExistsException(name)
            CreateStoreResponse.OutcomeCase.OUTCOME_NOT_SET, null -> error("Unexpected response: $response")
        }
    }

    suspend fun get(name: String): StoreInfo = grpcCall {
        val response = stub.withCallTimeout(callTimeout).getStore(getStoreRequest { this.name = name })
        when (response.outcomeCase) {
            GetStoreResponse.OutcomeCase.FOUND -> response.found.store.toModel()
            GetStoreResponse.OutcomeCase.NOT_FOUND -> throw StoreNotFoundException(name)
            GetStoreResponse.OutcomeCase.OUTCOME_NOT_SET, null -> error("Unexpected response: $response")
        }
    }

    suspend fun list(): List<StoreInfo> = grpcCall {
        stub.withCallTimeout(callTimeout).listStores(listStoresRequest {}).storesList.map { it.toModel() }
    }

    suspend fun delete(name: String): Unit = grpcCall {
        val response = stub.withCallTimeout(callTimeout).deleteStore(deleteStoreRequest { this.name = name })
        when (response.outcomeCase) {
            DeleteStoreResponse.OutcomeCase.DELETED -> Unit
            DeleteStoreResponse.OutcomeCase.NOT_FOUND -> throw StoreNotFoundException(name)
            DeleteStoreResponse.OutcomeCase.OUTCOME_NOT_SET, null -> error("Unexpected response: $response")
        }
    }

    suspend fun exists(name: String): Boolean = grpcCall {
        val response = stub.withCallTimeout(callTimeout).storeExists(storeExistsRequest { this.name = name })
        when (response.outcomeCase) {
            StoreExistsResponse.OutcomeCase.PRESENT -> true
            StoreExistsResponse.OutcomeCase.ABSENT -> false
            StoreExistsResponse.OutcomeCase.OUTCOME_NOT_SET, null -> error("Unexpected response: $response")
        }
    }
}
