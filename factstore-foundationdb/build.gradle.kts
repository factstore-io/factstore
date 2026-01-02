plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(project(":factstore-specification"))
    implementation(libs.org.jetbrains.kotlinx.kotlinx.coroutines.core)

    // foundation db client
    implementation(libs.org.foundationdb.fdb.java)

    // serialization
    implementation(libs.org.jetbrains.kotlinx.kotlinx.serialization.json)
    implementation(libs.com.github.avro.kotlin.avro4k.avro4k.core)

    // test
    testImplementation(libs.org.assertj.assertj.core)
    testImplementation(libs.earth.adi.testcontainers.foundationdb)
    testImplementation(libs.org.testcontainers.junit.jupiter)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.api)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.params)
    testRuntimeOnly(libs.org.junit.jupiter.junit.jupiter.engine)
    testRuntimeOnly(libs.org.junit.platform.junit.platform.launcher)
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

