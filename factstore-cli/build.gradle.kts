plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.allopen)
    alias(libs.plugins.quarkus)
    alias(libs.plugins.kotlin.serialization)
}

// Mordant, which renders the help and the tables, reaches the terminal through a native call.
// It ships three backends and tries them in a fixed order: FFM, GraalVM's FFI, then JNA.
//
//   JVM mode     the FFM backend, which every JVM this project supports provides. The JNA one
//                is left out: it calls a restricted method that Java 25 warns about on every
//                start, and it would be chosen only if FFM were missing anyway.
//   native mode  the GraalVM FFI backend, which is what it exists for. The FFM backend has to
//                be left out as well, because it loads inside a native image too and is tried
//                first -- and its downcalls would then need registering in
//                reachability-metadata.json, which the GraalVM one does not.
//
// Keyed on the setting the native build is invoked with, so neither mode needs a special
// command line of its own. Quarkus takes it as a system property, a Gradle property or an
// environment variable, and so does this: a native image that missed the exclusion would build
// fine and then fail on every command. Its config file is left out, since Gradle cannot read it
// here -- and set there, every build would be native, tests and dev mode included.
val nativeBuild = providers.systemProperty("quarkus.native.enabled")
    .orElse(providers.gradleProperty("quarkus.native.enabled"))
    .orElse(providers.environmentVariable("QUARKUS_NATIVE_ENABLED"))
    .orNull == "true"

dependencies {
    implementation(enforcedPlatform(libs.io.quarkus.platform.quarkus.bom))
    implementation("io.quarkus:quarkus-kotlin")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-config-yaml")
    implementation("io.quarkus:quarkus-grpc")

    // The command line itself; see the note on Mordant's backends above.
    implementation(libs.com.github.ajalt.clikt) {
        exclude(group = "com.github.ajalt.mordant", module = "mordant-jvm-jna")
        exclude(group = "com.github.ajalt.mordant", module = "mordant-jvm-jna-jvm")
        if (nativeBuild) {
            exclude(group = "com.github.ajalt.mordant", module = "mordant-jvm-ffm")
            exclude(group = "com.github.ajalt.mordant", module = "mordant-jvm-ffm-jvm")
        }
    }

    // The client library binds gRPC's shaded Netty, which Quarkus' native-image support does
    // not cover; the connection is opened on the plain transport Quarkus does configure.
    implementation(project(":factstore-client-kotlin")) {
        exclude(group = "io.grpc", module = "grpc-netty-shaded")
    }
    implementation("io.grpc:grpc-netty")
    implementation(libs.org.jetbrains.kotlinx.kotlinx.serialization.json)

    testImplementation(libs.org.junit.jupiter.junit.jupiter.api)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.engine)
    testImplementation(libs.org.junit.platform.junit.platform.launcher)
    testImplementation(libs.org.assertj.assertj.core)
}

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
}

// Quarkus needs CDI bean classes to be proxyable, which Kotlin classes are not by default.
allOpen {
    annotation("jakarta.enterprise.context.ApplicationScoped")
}

kotlin {
    compilerOptions {
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_25
        javaParameters = true
    }
}

tasks.test {
    useJUnitPlatform()
}
