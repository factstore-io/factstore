package io.factstore.client.operations

import io.factstore.client.internal.grpcCall
import io.factstore.client.internal.toDomain
import io.factstore.client.model.ServerInfo
import io.factstore.grpc.v1.InfoServiceGrpcKt.InfoServiceCoroutineStub
import io.factstore.grpc.v1.getServerInfoRequest
import java.time.Duration
import java.util.concurrent.TimeUnit

class InfoOperations internal constructor(
    private val stub: InfoServiceCoroutineStub,
    private val callTimeout: Duration,
) {
    suspend fun getServerInfo(): ServerInfo = grpcCall {
        stub.withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)
            .getServerInfo(getServerInfoRequest {})
            .toDomain()
    }
}
