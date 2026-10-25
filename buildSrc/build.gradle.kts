plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

dependencies {
    // Puts the Kotlin Gradle plugin on every build script's classpath, at the catalog's version.
    implementation(libs.kotlin.gradle.plugin)
}
