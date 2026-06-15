package io.factstore.client

import java.io.File

sealed class TlsConfig {

    /** Plain-text, no TLS. */
    data object Disabled : TlsConfig()

    /** TLS using the JVM's default system trust store. */
    data object SystemDefault : TlsConfig()

    /**
     * TLS with explicit certificate material.
     *
     * [trustCertCollectionFile] overrides the CA bundle used to verify the server.
     * Supply [certChainFile] and [privateKeyFile] together to enable mutual TLS (mTLS).
     */
    data class Custom(
        val trustCertCollectionFile: File? = null,
        val certChainFile: File? = null,
        val privateKeyFile: File? = null,
    ) : TlsConfig()
}
