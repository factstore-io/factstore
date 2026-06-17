package io.factstore.client.operations

import io.factstore.client.exceptions.StoreNameAlreadyExistsException
import io.factstore.client.exceptions.StoreNotFoundException
import io.factstore.client.internal.grpcCall
import io.factstore.client.internal.toDomain
import io.factstore.client.model.StoreInfo
import io.factstore.grpc.v1.StoreServiceGrpcKt.StoreServiceCoroutineStub
import io.factstore.grpc.v1.createStoreRequest
import io.factstore.grpc.v1.deleteStoreRequest
import io.factstore.grpc.v1.getStoreRequest
import io.factstore.grpc.v1.listStoresRequest
import io.factstore.grpc.v1.storeExistsRequest
import java.time.Duration
import java.util.concurrent.TimeUnit

class StoreOperations internal constructor(
    private val stub: StoreServiceCoroutineStub,
    private val callTimeout: Duration,
) {
    private fun timedStub() = stub.withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)

    suspend fun create(name: String): String = grpcCall {
        val response = timedStub().createStore(createStoreRequest { this.name = name })
        when {
            response.hasCreated() -> response.created.id
            response.hasNameAlreadyExists() -> throw StoreNameAlreadyExistsException(name)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun get(name: String): StoreInfo = grpcCall {
        val response = timedStub().getStore(getStoreRequest { this.name = name })
        when {
            response.hasFound() -> response.found.store.toDomain()
            response.hasNotFound() -> throw StoreNotFoundException(name)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun list(): List<StoreInfo> = grpcCall {
        timedStub().listStores(listStoresRequest {}).storesList.map { it.toDomain() }
    }

    suspend fun delete(name: String): Unit = grpcCall {
        val response = timedStub().deleteStore(deleteStoreRequest { this.name = name })
        when {
            response.hasDeleted() -> Unit
            response.hasNotFound() -> throw StoreNotFoundException(name)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun exists(name: String): Boolean = grpcCall {
        timedStub().storeExists(storeExistsRequest { this.name = name }).hasPresent()
    }
}
