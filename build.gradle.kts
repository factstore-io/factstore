plugins {
    alias(libs.plugins.kotlin.jvm) apply false
}

// Overridable so release builds can stamp the git tag into the artifacts:
//   ./gradlew build -PfactstoreVersion=0.1.0-alpha
// This reaches quarkus.application.version, which the server reports on /info.
val factstoreVersion = providers.gradleProperty("factstoreVersion").getOrElse("0.1.0-SNAPSHOT")

subprojects {

    repositories {
        mavenCentral()
    }

    group = "org.factstore"
    version = factstoreVersion
}
