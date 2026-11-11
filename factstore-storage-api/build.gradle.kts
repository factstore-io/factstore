plugins {
    id("factstore.kotlin-jvm")
    `java-test-fixtures`
}

dependencies {
    api(project(":factstore-specification"))
    api(libs.kotlinx.coroutines.core)

    testFixturesApi(libs.bundles.testing)
}
