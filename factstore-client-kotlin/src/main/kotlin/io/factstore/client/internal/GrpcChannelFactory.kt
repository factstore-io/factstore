package io.factstore.client.internal

import io.factstore.client.FactStoreClientConfig
import io.factstore.client.TlsConfig
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

internal object GrpcChannelFactory {

    fun create(config: FactStoreClientConfig): ManagedChannel = when (config.tls) {
        TlsConfig.Disabled -> ManagedChannelBuilder
            .forAddress(config.host, config.port)
            .usePlaintext()
            .build()

        TlsConfig.SystemDefault -> NettyChannelBuilder
            .forAddress(config.host, config.port)
            .build()

        is TlsConfig.Custom -> {
            val sslContext = GrpcSslContexts.forClient().run {
                config.tls.trustCertCollectionFile?.let { trustManager(it) }
                if (config.tls.certChainFile != null && config.tls.privateKeyFile != null) {
                    keyManager(config.tls.certChainFile, config.tls.privateKeyFile)
                }
                build()
            }
            NettyChannelBuilder
                .forAddress(config.host, config.port)
                .sslContext(sslContext)
                .build()
        }
    }
}
