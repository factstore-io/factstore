package io.factstore.client

import io.factstore.client.internal.GrpcChannelFactory
import io.factstore.client.operations.FactOperations
import io.factstore.client.operations.InfoOperations
import io.factstore.client.operations.StoreOperations
import io.factstore.grpc.v1.FactServiceGrpcKt.FactServiceCoroutineStub
import io.factstore.grpc.v1.InfoServiceGrpcKt.InfoServiceCoroutineStub
import io.factstore.grpc.v1.StoreServiceGrpcKt.StoreServiceCoroutineStub
import io.grpc.Channel
import io.grpc.ManagedChannel
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.TimeUnit

class FactStoreClient private constructor(
    channel: Channel,
    callTimeout: Duration,
    private val ownedChannel: ManagedChannel?,
) : Closeable {

    val stores = StoreOperations(StoreServiceCoroutineStub(channel), callTimeout)
    val facts = FactOperations(FactServiceCoroutineStub(channel), callTimeout)
    val info = InfoOperations(InfoServiceCoroutineStub(channel), callTimeout)

    override fun close() {
        ownedChannel?.shutdown()?.awaitTermination(5, TimeUnit.SECONDS)
    }

    companion object {
        operator fun invoke(config: FactStoreClientConfig): FactStoreClient {
            val channel = GrpcChannelFactory.create(config)
            return FactStoreClient(channel, config.callTimeout, channel)
        }

        operator fun invoke(
            channel: Channel,
            callTimeout: Duration = Duration.ofSeconds(30),
        ): FactStoreClient = FactStoreClient(channel, callTimeout, null)
    }
}

fun factStoreClient(block: FactStoreClientConfigBuilder.() -> Unit): FactStoreClient =
    FactStoreClient(FactStoreClientConfigBuilder().apply(block).build())

fun factStoreClient(channel: Channel, callTimeout: Duration = Duration.ofSeconds(30)): FactStoreClient =
    FactStoreClient(channel, callTimeout)
