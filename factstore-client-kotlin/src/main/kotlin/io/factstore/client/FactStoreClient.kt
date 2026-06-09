package io.factstore.client

import io.factstore.client.internal.GrpcChannelFactory
import io.factstore.client.operations.FactOperations
import io.factstore.client.operations.InfoOperations
import io.factstore.client.operations.StoreOperations
import io.factstore.grpc.v1.FactServiceGrpcKt.FactServiceCoroutineStub
import io.factstore.grpc.v1.InfoServiceGrpcKt.InfoServiceCoroutineStub
import io.factstore.grpc.v1.StoreServiceGrpcKt.StoreServiceCoroutineStub
import io.grpc.ManagedChannel
import java.io.Closeable
import java.util.concurrent.TimeUnit

class FactStoreClient(config: FactStoreClientConfig) : Closeable {

    private val channel: ManagedChannel = GrpcChannelFactory.create(config)

    val stores = StoreOperations(StoreServiceCoroutineStub(channel), config.callTimeout)
    val facts = FactOperations(FactServiceCoroutineStub(channel), config.callTimeout)
    val info = InfoOperations(InfoServiceCoroutineStub(channel), config.callTimeout)

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

fun factStoreClient(block: FactStoreClientConfigBuilder.() -> Unit): FactStoreClient =
    FactStoreClient(FactStoreClientConfigBuilder().apply(block).build())
