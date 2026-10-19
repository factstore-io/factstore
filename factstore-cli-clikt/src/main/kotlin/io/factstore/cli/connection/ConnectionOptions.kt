package io.factstore.cli.connection

import com.github.ajalt.clikt.parameters.groups.OptionGroup
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.validate
import java.net.URI

internal val DEFAULT_URL: URI = URI.create("http://localhost:8080")

/**
 * Which server a command talks to, and with which defaults.
 *
 * Resolved the way `kubectl` and `aws` resolve theirs: an explicit flag wins, then the
 * environment, then the selected context in `~/.factstore/config`, then the built-in default.
 * Clikt could read the file declaratively through a `ValueSource`, but the section to read
 * depends on `--context`, which is itself an option -- so the last two steps are resolved here,
 * in the one place that knows the whole chain.
 */
class ConnectionOptions(
    private val contexts: FactStoreContexts = FactStoreContexts.fromUserHome(),
) : OptionGroup(name = "Connection options") {

    private val contextName by option(
        "--context",
        metavar = "<name>",
        envvar = "FACTSTORE_CONTEXT",
        help = "The context in ~/.factstore/config to use (default: its current-context)",
    ).validate { name ->
        // An unknown name would otherwise resolve to no url and no store, and the defaults would
        // send a command meant for another server to the local one.
        require(contexts.defines(name)) { "no context '$name' in ~/.factstore/config" }
    }

    private val url by option(
        "--url",
        metavar = "<url>",
        envvar = "FACTSTORE_URL",
        help = "The FactStore server to talk to (default: the context's url, or $DEFAULT_URL)",
    ).convert { value ->
        val url = runCatching { URI.create(value) }.getOrNull()
        if (url?.host == null || url.scheme == null) {
            fail("expected a URL with a scheme and a host, like http://localhost:8080, but was '$value'")
        }
        url
    }

    private val context: FactStoreContext by lazy { contexts.context(contextName) }

    val connection: Connection by lazy { Connection(url ?: context.url ?: DEFAULT_URL) }

    /** The store to work with when a command was not given one. */
    val defaultStore: String? get() = context.store
}
