plugins {
    id("factstore.kotlin-jvm")
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    api(project(":factstore-domain"))
    api(project(":factstore-storage-api"))
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(project(":factstore-storage-memory"))
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}
