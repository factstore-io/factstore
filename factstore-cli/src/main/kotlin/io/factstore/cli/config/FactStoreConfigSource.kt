package io.factstore.cli.config

import org.eclipse.microprofile.config.spi.ConfigSource
import java.io.File
import java.net.URI
import java.util.Properties

const val ORDINAL = 275

class FactStoreConfigSource : ConfigSource {

    private val entries: Map<String, String> by lazy { loadAndTranslate() }

    private fun loadAndTranslate(): Map<String, String> {
        val configFile = File(File(System.getProperty("user.home"), ".factstore"), "config")
        if (!configFile.exists()) return emptyMap()

        val raw = Properties().apply { configFile.inputStream().use { load(it) } }
        val result = LinkedHashMap<String, String>()

        raw.getProperty("url")?.trim()?.takeIf { it.isNotEmpty() }?.let { result += urlToGrpcKeys(it) }
        raw.getProperty("store")?.trim()?.let { result["factstore.store"] = it }
        raw.getProperty("api-key")?.trim()?.let { result["factstore.api-key"] = it }

        return result
    }

    private fun urlToGrpcKeys(urlString: String): Map<String, String> {
        val uri = URI.create(urlString)
        val host = uri.host ?: return emptyMap()
        val tls = uri.scheme.equals("https", ignoreCase = true)
        val port = if (uri.port != -1) uri.port else if (tls) 443 else 80
        return mapOf(
            "quarkus.grpc.clients.factstore.host" to host,
            "quarkus.grpc.clients.factstore.port" to port.toString(),
            // verify this key for your version; see the TLS note below
            "quarkus.grpc.clients.factstore.plain-text" to (!tls).toString(),
        )
    }

    override fun getPropertyNames(): Set<String> = entries.keys
    override fun getValue(propertyName: String?): String? = propertyName?.let { entries[it] }
    override fun getName(): String = "factstore-config-file"
    override fun getOrdinal(): Int = ORDINAL

}