plugins {
    id("factstore.kotlin-jvm")
}

dependencies {
    implementation(project(":factstore-specification"))
    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}
