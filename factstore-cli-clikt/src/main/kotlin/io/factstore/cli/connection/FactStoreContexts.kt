package io.factstore.cli.connection

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
 * A missing file, a missing context or a missing key is not an error: each one simply leaves the
 * setting unset, for the caller's default to fill in.
 */
class FactStoreContexts(private val values: Map<String, String>) {

    /** The context called [name], or the file's `current-context` when [name] is `null`. */
    fun context(name: String? = null): FactStoreContext {
        val selected = name ?: values[CURRENT_CONTEXT] ?: return FactStoreContext.Unset
        return FactStoreContext(
            name = selected,
            url = values["context.$selected.url"]?.let(URI::create),
            store = values["context.$selected.store"],
        )
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

        fun fromUserHome(): FactStoreContexts = from(Path.of(userHome(), ".factstore", "config"))

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
