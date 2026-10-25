import io.quarkus.gradle.tasks.QuarkusDev
import io.quarkus.gradle.tasks.QuarkusRun

plugins {
    id("factstore.kotlin-jvm")
    alias(libs.plugins.kotlin.allopen)
    alias(libs.plugins.quarkus)
}

dependencies {
    implementation(enforcedPlatform(libs.quarkus.bom))
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-rest-jackson")
    implementation("io.quarkus:quarkus-grpc")
    implementation("io.quarkus:quarkus-vertx-kotlin")
    implementation("io.vertx:vertx-lang-kotlin-coroutines")
    implementation("io.quarkus:quarkus-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-config-yaml")
    implementation("io.quarkus:quarkus-smallrye-openapi")
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("io.quarkus:quarkus-container-image-docker")
    implementation(libs.quarkus.quinoa)
    implementation("com.google.protobuf:protobuf-kotlin")
    implementation(libs.kotlinx.coroutines.jdk9)

    // factstore libs
    implementation(project(":factstore-proto"))
    implementation(project(":factstore-specification"))
    implementation(project(":factstore-foundationdb"))
    implementation(project(":factstore-memory"))

    implementation(libs.kotlin.logging)

    testImplementation("io.quarkus:quarkus-junit")
    testImplementation("io.rest-assured:rest-assured")
    testImplementation(libs.assertj.core)
}

allOpen {
    annotation("jakarta.ws.rs.Path")
    annotation("jakarta.enterprise.context.ApplicationScoped")
    annotation("jakarta.persistence.Entity")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

kotlin {
    compilerOptions {
        // Quarkus reads parameter names by reflection, for CDI and REST.
        javaParameters = true
    }
}

val fdbJvmArgs = listOf(
    "--enable-native-access=ALL-UNNAMED",
    "--sun-misc-unsafe-memory-access=allow"
)

tasks.withType<QuarkusDev>().configureEach {
    jvmArgs = fdbJvmArgs
}

tasks.withType<QuarkusRun>().configureEach {
    jvmArgs = fdbJvmArgs
}
