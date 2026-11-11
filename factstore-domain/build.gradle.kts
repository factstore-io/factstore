plugins {
    id("factstore.kotlin-jvm")
}

dependencies {
    api(project(":factstore-specification"))
    api(libs.fmodel.domain)
    implementation(libs.kotlinx.coroutines.core)

    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}
