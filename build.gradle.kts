plugins {
    alias(libs.plugins.kotlin.jvm) apply false
}

subprojects {

    repositories {
        mavenCentral()
    }

    group = "org.factstore"
    version = "1.0.0-SNAPSHOT"
}
