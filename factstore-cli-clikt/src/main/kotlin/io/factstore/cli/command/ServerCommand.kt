package io.factstore.cli.command

import com.github.ajalt.clikt.command.SuspendingCliktCommand
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.parameters.groups.provideDelegate
import com.github.ajalt.clikt.parameters.options.option
import io.factstore.cli.connection.ConnectionOptions
import io.factstore.cli.connection.FactStoreClients
import io.factstore.client.FactStoreClient

/**
 * A command that talks to a FactStore server.
 *
 * Everything about *which* server, and how to reach it, lives here and in [ConnectionOptions]:
 * a command below only reads [client] and calls the client library with what it was given.
 */
abstract class ServerCommand(
    name: String,
    private val clients: FactStoreClients,
) : SuspendingCliktCommand(name) {

    private val connectionOptions by ConnectionOptions()

    /** The server this invocation talks to, connected when a command first reads it. */
    protected val client: FactStoreClient by lazy { clients.connect(connectionOptions.connection) }

    /** The store to work with when no command line said which, from the selected context. */
    protected val contextStore: String? get() = connectionOptions.defaultStore
}

/**
 * A command that works within one store.
 *
 * `--store` is resolved on the same terms as the connection: the flag, then the environment,
 * then the context in the config file.
 */
abstract class StoreScopedCommand(
    name: String,
    clients: FactStoreClients,
) : ServerCommand(name, clients) {

    private val store by option(
        "--store", "-s",
        metavar = "<name>",
        envvar = "FACTSTORE_STORE",
        help = "The store to work with (default: the context's store)",
    )

    protected val storeName: String
        get() = store ?: contextStore ?: throw UsageError(
            "no store given: pass --store, set FACTSTORE_STORE, " +
                "or give the context a store in ~/.factstore/config"
        )
}
