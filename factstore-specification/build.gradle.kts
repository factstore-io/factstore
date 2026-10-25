plugins {
    id("factstore.kotlin-jvm")
}

dependencies {
    implementation(libs.kotlinx.coroutines.core)

    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}
