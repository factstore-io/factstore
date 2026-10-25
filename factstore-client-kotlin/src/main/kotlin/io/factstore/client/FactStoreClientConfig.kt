package io.factstore.client

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class FactStoreClientConfig(
    val host: String = DEFAULT_HOST,
    val port: Int = DEFAULT_PORT,
    val tls: TlsConfig = TlsConfig.Disabled,
    val callTimeout: Duration = DEFAULT_CALL_TIMEOUT,
) {

    companion object {
        const val DEFAULT_HOST = "localhost"
        const val DEFAULT_PORT = 9000

        /** How long a unary call may take before it fails; streams are not limited. */
        val DEFAULT_CALL_TIMEOUT = 30.seconds
    }
}

@FactStoreDsl
class FactStoreClientConfigBuilder {
    var host: String = FactStoreClientConfig.DEFAULT_HOST
    var port: Int = FactStoreClientConfig.DEFAULT_PORT
    var tls: TlsConfig = TlsConfig.Disabled
    var callTimeout: Duration = FactStoreClientConfig.DEFAULT_CALL_TIMEOUT

    fun build(): FactStoreClientConfig = FactStoreClientConfig(host, port, tls, callTimeout)
}
