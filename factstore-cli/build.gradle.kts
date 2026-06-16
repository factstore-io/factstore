plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.allopen)
    alias(libs.plugins.quarkus)
    alias(libs.plugins.kotlin.serialization)
}

val protobufVersion = libs.versions.google.protobuf.get()

configurations.all {
    resolutionStrategy {
        force("com.google.protobuf:protobuf-java:$protobufVersion")
        force("com.google.protobuf:protobuf-kotlin:$protobufVersion")
    }
}

dependencies {
    implementation(enforcedPlatform(libs.io.quarkus.platform.quarkus.bom))
    implementation("io.quarkus:quarkus-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-config-yaml")
    implementation("io.quarkus:quarkus-picocli")
    implementation("io.quarkus:quarkus-grpc")

    implementation(project(":factstore-client-kotlin"))

    implementation(libs.io.github.oshai.kotlin.logging)

    testImplementation("io.quarkus:quarkus-junit")
    testImplementation(libs.org.assertj.assertj.core)
}

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
}

allOpen {
    annotation("jakarta.ws.rs.Path")
    annotation("jakarta.enterprise.context.ApplicationScoped")
    annotation("jakarta.persistence.Entity")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

kotlin {
    compilerOptions {
        jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_25
        javaParameters = true
    }
}
