pluginManagement {
    val quarkusPluginVersion: String by settings
    val quarkusPluginId: String by settings
    repositories {
        mavenCentral()
        gradlePluginPortal()
        mavenLocal()
    }
    plugins {
        id(quarkusPluginId) version quarkusPluginVersion
    }
}
rootProject.name="factstore"

include(
    "factstore-specification",
    "factstore-foundationdb",
    "factstore-avro",
    "factstore-fmodel",
    "fact-explorer"
)
