plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    // Generated stubs (and the grpc/protobuf runtime they expose) come from
    // :factstore-proto. `api` so that callers holding a FactStoreClient can name
    // the proto types it returns.
    api(project(":factstore-proto"))
    api(libs.org.jetbrains.kotlinx.kotlinx.serialization.json)
    // Version supplied by the Quarkus BOM, which :factstore-proto exports.
    implementation("io.grpc:grpc-netty-shaded")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    compilerOptions { freeCompilerArgs.add("-opt-in=kotlin.RequiresOptIn") }
}

kotlin {
    jvmToolchain(25)
}
