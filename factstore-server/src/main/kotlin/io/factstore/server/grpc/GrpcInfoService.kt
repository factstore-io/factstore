package io.factstore.server.grpc

import io.factstore.grpc.v1.FactStoreProto
import io.factstore.grpc.v1.InfoServiceGrpcKt
import io.factstore.grpc.v1.serverInfo
import io.quarkus.grpc.GrpcService
import io.factstore.server.info.ServerInfo as CoreServerInfo

@GrpcService
class GrpcInfoService(
    private val info: CoreServerInfo,
) : InfoServiceGrpcKt.InfoServiceCoroutineImplBase() {

    override suspend fun getServerInfo(request: FactStoreProto.GetServerInfoRequest): FactStoreProto.ServerInfo =
        serverInfo {
            app = info.app
            version = info.version
            storageBackend = info.storageBackend
        }
}
