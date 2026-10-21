package io.factstore.cli.connection

import com.github.ajalt.clikt.core.CliktError
import java.net.URI
import java.nio.file.Path
import java.util.Properties
import kotlin.io.path.exists
import kotlin.io.path.inputStream

/** One named set of connection settings, as `~/.factstore/config` holds them. */
data class FactStoreContext(
    val name: String?,
    val url: URI?,
    val store: String?,
) {
    companion object {
        /** What is known about the connection when there is no config file. */
        val Unset = FactStoreContext(name = null, url = null, store = null)
    }
}

/**
 * The contexts in `~/.factstore/config`, in the shape `kubectl` and `aws` taught everyone to
 * expect: several named servers, one of them current.
 *
 * ```properties
 * current-context = local
 *
 * context.local.url = http://localhost:8080
 * context.local.store = demo-shop
 *
 * context.prod.url = https://facts.internal:443
 * ```
 *
 * A missing file or a missing key is not an error: each one simply leaves the setting unset, for
 * the caller's default to fill in. A context the file does not define is a different matter: it
 * would resolve to no url and no store, and the defaults would send a command meant for another
 * server to the local one. [defines] is how a caller that was given a name can say so, and a
 * `current-context` naming no context is an error when it is read, as is a url that is not one.
 */
class FactStoreContexts(private val values: Map<String, String>) {

    /** Whether the file defines a context called [name]. */
    fun defines(name: String): Boolean = values.keys.any { it.startsWith("context.$name.") }

    /**
     * The context called [name], or the file's `current-context` when [name] is `null`.
     *
     * @throws CliktError if `current-context` names no context, or the context's url is not the
     *   URL of a server.
     */
    fun context(name: String? = null): FactStoreContext {
        val selected = name ?: currentContext() ?: return FactStoreContext.Unset
        return FactStoreContext(
            name = selected,
            url = url(key = "context.$selected.url"),
            store = values["context.$selected.store"],
        )
    }

    private fun currentContext(): String? = values[CURRENT_CONTEXT]?.also { name ->
        if (!defines(name)) {
            throw CliktError("Error: current-context in ~/.factstore/config is '$name', but there is no such context")
        }
    }

    // Not a UsageError: Clikt would print the usage line above it, and the command line is fine.
    private fun url(key: String): URI? = values[key]?.let { value ->
        serverUrlOrNull(value) ?: throw CliktError("Error: $key in ~/.factstore/config: ${notAServerUrl(value)}")
    }

    companion object {
        private const val CURRENT_CONTEXT = "current-context"

        val Empty = FactStoreContexts(emptyMap())

        /** Reads [path], or returns [Empty] if it does not exist. */
        fun from(path: Path): FactStoreContexts {
            if (!path.exists()) return Empty
            val properties = Properties().apply { path.inputStream().use(::load) }
            return FactStoreContexts(properties.entries.associate { (k, v) -> "$k".trim() to "$v".trim() })
        }

        /**
         * The user's own contexts, read once.
         *
         * Every command of the tree is built before one of them runs, each with its own
         * connection options, and they all read the same file.
         */
        fun fromUserHome(): FactStoreContexts = userHomeContexts

        private val userHomeContexts: FactStoreContexts by lazy {
            from(Path.of(userHome(), ".factstore", "config"))
        }

        /**
         * The home directory as the shell means it by `~`.
         *
         * The environment is asked before `user.home`, because a native image bakes that
         * property while it is built -- in a container build, that is the builder's home and
         * not the user's, and the config file would never be found.
         */
        private fun userHome(): String =
            System.getenv("HOME")
                ?: System.getenv("USERPROFILE")
                ?: System.getProperty("user.home")
    }
}
