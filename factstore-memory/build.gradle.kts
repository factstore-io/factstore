plugins {
    id("factstore.kotlin-jvm")
}

dependencies {
    implementation(project(":factstore-specification"))
    implementation(libs.kotlinx.coroutines.core)

    testImplementation(project(":factstore-testing"))
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}
