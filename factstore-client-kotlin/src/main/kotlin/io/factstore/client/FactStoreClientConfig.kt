package io.factstore.client

import java.time.Duration

data class FactStoreClientConfig(
    val host: String,
    val port: Int = 9000,
    val tls: TlsConfig = TlsConfig.Disabled,
    val callTimeout: Duration = Duration.ofSeconds(30),
)

class FactStoreClientConfigBuilder {
    var host: String = "localhost"
    var port: Int = 9000
    var tls: TlsConfig = TlsConfig.Disabled
    var callTimeout: Duration = Duration.ofSeconds(30)

    fun build(): FactStoreClientConfig = FactStoreClientConfig(host, port, tls, callTimeout)
}
