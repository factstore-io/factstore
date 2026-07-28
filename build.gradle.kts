plugins {
    alias(libs.plugins.kotlin.jvm) apply false
}

subprojects {

    repositories {
        mavenCentral()
    }

    group = "org.factstore"
    version = "0.1.0-SNAPSHOT"
}
