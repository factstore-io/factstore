package io.factstore.client.operations

import io.factstore.client.internal.grpcCall
import io.factstore.client.internal.toModel
import io.factstore.client.internal.withCallTimeout
import io.factstore.client.model.ServerInfo
import io.factstore.grpc.v1.InfoServiceGrpcKt.InfoServiceCoroutineStub
import io.factstore.grpc.v1.getServerInfoRequest
import kotlin.time.Duration

class InfoOperations internal constructor(
    private val stub: InfoServiceCoroutineStub,
    private val callTimeout: Duration,
) {
    suspend fun getServerInfo(): ServerInfo = grpcCall {
        stub.withCallTimeout(callTimeout).getServerInfo(getServerInfoRequest {}).toModel()
    }
}
