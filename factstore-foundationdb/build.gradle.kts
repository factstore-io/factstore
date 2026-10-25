plugins {
    id("factstore.kotlin-jvm")
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    constraints {
        implementation(libs.jackson.databind) {
            because("avro 1.12.1 constrains jackson-databind to 2.20.0, which is vulnerable")
        }
    }

    implementation(project(":factstore-specification"))
    implementation(libs.kotlinx.coroutines.core)

    // foundation db client
    implementation(libs.fdb.java)

    // serialization
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.avro4k.core)

    // test
    testImplementation(project(":factstore-testing"))
    testImplementation(libs.testcontainers.foundationdb)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    jvmArgs(
        "--enable-native-access=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow"
    )
}
