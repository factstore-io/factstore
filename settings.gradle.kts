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
rootProject.name="fact-store"

include(
    "fact-store-specification",
    "fact-store-foundationdb",
    "fact-store-avro",
    "fact-store-fmodel",
    "fact-explorer"
)
