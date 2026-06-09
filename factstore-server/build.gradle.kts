import io.quarkus.gradle.tasks.QuarkusDev
import io.quarkus.gradle.tasks.QuarkusRun

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.allopen)
    alias(libs.plugins.quarkus)
}

dependencies {
    implementation(enforcedPlatform(libs.io.quarkus.platform.quarkus.bom))
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-rest-jackson")
    implementation("io.quarkus:quarkus-grpc")
    implementation("io.quarkus:quarkus-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-config-yaml")
    implementation("io.quarkus:quarkus-hibernate-validator")
    implementation("io.quarkus:quarkus-smallrye-openapi")
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation(libs.quarkus.quinoa)
    implementation(libs.com.google.protobuf.protobuf.kotlin)
    implementation("io.vertx:vertx-lang-kotlin-coroutines")
    implementation(libs.coroutines.jdk)


    // factstore libs
    implementation(project(":factstore-specification"))
    implementation(project(":factstore-foundationdb"))
    implementation(project(":factstore-memory"))

    implementation(libs.io.github.oshai.kotlin.logging)

    testImplementation("io.quarkus:quarkus-junit")
    testImplementation("io.rest-assured:rest-assured")
    testImplementation(libs.org.assertj.assertj.core)
    testImplementation(libs.io.smallrye.mutiny.kotlin)
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

sourceSets {
    main {
        java {
            srcDir("build/classes/java/quarkus-generated-sources/grpc")
        }
    }
}
