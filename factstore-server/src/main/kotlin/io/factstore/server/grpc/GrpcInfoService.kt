package io.factstore.server.grpc

import io.quarkus.grpc.GrpcService
import io.smallrye.mutiny.Uni
import io.factstore.server.info.ServerInfo as CoreServerInfo

@GrpcService
class GrpcInfoService(private val info: CoreServerInfo) : InfoService {

    override fun getServerInfo(request: FactStoreProto.GetServerInfoRequest): Uni<FactStoreProto.ServerInfo> =
        Uni.createFrom().item(
            serverInfo {
                app = info.app
                version = info.version
                storageBackend = info.storageBackend
            }
        )
}
