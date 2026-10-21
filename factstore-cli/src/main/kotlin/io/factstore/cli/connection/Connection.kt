package io.factstore.cli.connection

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import java.net.URI

private const val HTTPS_PORT = 443
private const val HTTP_PORT = 80

/**
 * [value] as the URL of a server, or `null` if it is not one.
 *
 * A connection needs a scheme and a host, and [URI] accepts far less: `facts.internal:443`
 * parses, with `facts.internal` as its scheme and no host at all.
 */
internal fun serverUrlOrNull(value: String): URI? =
    runCatching { URI.create(value) }.getOrNull()?.takeIf { it.scheme != null && it.host != null }

/** Why [value] was refused by [serverUrlOrNull]. */
internal fun notAServerUrl(value: String): String =
    "expected a URL with a scheme and a host, like http://localhost:8080, but was '$value'"

/**
 * A FactStore server to talk to.
 *
 * The scheme decides whether the connection is encrypted, and a URL without a port falls back to
 * the scheme's own default, so `https://facts.example.com` and `http://localhost:8080` both say
 * everything they need to.
 */
data class Connection(val url: URI) {

    private val encrypted: Boolean get() = url.scheme.equals("https", ignoreCase = true)

    private val port: Int get() = if (url.port != -1) url.port else if (encrypted) HTTPS_PORT else HTTP_PORT

    /**
     * Opens the channel to this server.
     *
     * Built on gRPC's plain Netty transport rather than through the client library's own
     * configuration, which reaches for the shaded one: Quarkus configures this transport for
     * native images, and the shaded copy would have to be taught class by class.
     */
    internal fun openChannel(): ManagedChannel =
        NettyChannelBuilder.forAddress(url.host, port)
            .apply { if (encrypted) useTransportSecurity() else usePlaintext() }
            .build()

    override fun toString(): String = url.toString()
}
