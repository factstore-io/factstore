package io.factstore.cli.config

import jakarta.enterprise.context.ApplicationScoped
import java.io.File
import java.util.Properties

@ApplicationScoped
class ConfigService {

    private val configDir = File(System.getProperty("user.home"), ".factstore")
    private val configFile = File(configDir, "config")

    private val properties: Properties? by lazy {
        if (!configFile.exists()) null
        else configFile.inputStream().use { stream -> Properties().also { it.load(stream) } }
    }

    fun getUrl(): String? = properties?.getProperty("url")

    fun getStore(): String? = properties?.getProperty("store")

}
