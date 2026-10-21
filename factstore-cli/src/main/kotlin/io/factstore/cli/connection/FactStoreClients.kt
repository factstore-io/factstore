package io.factstore.cli.connection

import io.factstore.client.FactStoreClient
import io.grpc.ManagedChannel
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

private const val SHUTDOWN_SECONDS = 5L

/**
 * Opens and owns the clients the commands use.
 *
 * A client is opened on first use, so a command that never reaches the server -- `--help`, a
 * usage error -- never opens a connection, and both the client and its channel are shut down
 * with the application.
 */
@ApplicationScoped
class FactStoreClients {

    private val channels = ConcurrentHashMap<Connection, ManagedChannel>()
    private val clients = ConcurrentHashMap<Connection, FactStoreClient>()

    fun connect(connection: Connection): FactStoreClient =
        clients.computeIfAbsent(connection) {
            useNioTransport()
            FactStoreClient(channels.computeIfAbsent(it) { target -> target.openChannel() })
        }

    @PreDestroy
    fun close() {
        clients.values.forEach(FactStoreClient::close)
        clients.clear()
        // The clients were handed a channel rather than a target, so closing them leaves the
        // channels open: they belong to whoever opened them.
        channels.values.forEach { it.shutdown().awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS) }
        channels.clear()
    }

    /**
     * Keeps gRPC on Netty's NIO transport rather than its native one.
     *
     * Loading the native transport is a restricted operation, which Java warns about on stderr
     * unless the launcher passes `--enable-native-access` -- four lines in front of every
     * command's output, and a CLI cannot choose its own launcher. Native epoll buys a
     * short-lived command with one or two calls nothing, so it is declined here, in the one
     * place that opens a channel. The property has to be set before the first one is built.
     */
    private fun useNioTransport() {
        System.setProperty("io.netty.transport.noNative", "true")
    }
}
