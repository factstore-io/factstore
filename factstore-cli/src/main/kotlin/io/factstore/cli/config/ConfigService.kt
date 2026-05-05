package io.factstore.cli.config

import jakarta.enterprise.context.ApplicationScoped
import java.io.File
import java.util.Properties

@ApplicationScoped
class ConfigService {

    private val configDir = File(System.getProperty("user.home"), ".factstore")
    private val configFile = File(configDir, "config")

    fun getUrl(): String? {
        if (!configFile.exists()) return null
        val props = Properties()
        configFile.inputStream().use { props.load(it) }
        return props.getProperty("url")
    }

}
