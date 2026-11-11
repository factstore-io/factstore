plugins {
    id("factstore.kotlin-jvm")
}

dependencies {
    api(project(":factstore-storage-api"))
    implementation(libs.kotlinx.coroutines.core)

    testImplementation(testFixtures(project(":factstore-storage-api")))
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}
