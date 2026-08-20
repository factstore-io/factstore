plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.protobuf)
}

// protoc and the grpc-java plugin are pinned by the Quarkus BOM rather than by this
// project, so generated code can never be newer than the runtime a Quarkus module
// forces onto it. protobuf 4.x enforces gencode <= runtime at class-init, so a skew
// here is a startup crash, not a warning.
val quarkusManaged = configurations.detachedConfiguration(
    dependencies.enforcedPlatform(libs.io.quarkus.platform.quarkus.bom.get()),
    dependencies.create("com.google.protobuf:protobuf-java"),
    dependencies.create("io.grpc:grpc-stub"),
)

// resolutionResult reads metadata only -- no jars are downloaded to learn a version.
val managed: Map<String, String> =
    quarkusManaged.incoming.resolutionResult.allComponents
        .mapNotNull { it.moduleVersion }
        .associate { "${it.group}:${it.name}" to it.version }

val protobufVersion = managed.getValue("com.google.protobuf:protobuf-java")
val grpcVersion = managed.getValue("io.grpc:grpc-stub")

logger.info("factstore-proto: protobuf=$protobufVersion grpc=$grpcVersion (from quarkus-bom)")

dependencies {
    // The same BOM governs the runtime artifacts, and `api` propagates those
    // constraints to every consumer, so the client cannot drift from the server.
    // `platform`, not `enforcedPlatform`: these are published artifacts, and a library
    // should recommend versions to its consumers, not force them.
    api(platform(libs.io.quarkus.platform.quarkus.bom))
    api("io.grpc:grpc-stub")
    api("io.grpc:grpc-protobuf")
    api("com.google.protobuf:protobuf-kotlin")
    // grpc-kotlin is not in the Quarkus BOM -- Quarkus has no coroutine stub support,
    // so this is the one version this project must choose for itself.
    api(libs.io.grpc.grpc.kotlin.stub)
    api(libs.org.jetbrains.kotlinx.kotlinx.coroutines.core)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
        create("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:${libs.versions.grpcKotlin.get()}:jdk8@jar"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
                create("grpckt")
            }
            task.builtins {
                create("kotlin")
            }
        }
    }
}

kotlin {
    jvmToolchain(25)
}
