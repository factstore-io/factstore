plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.protobuf)
}

dependencies {
    implementation(libs.io.grpc.grpc.stub)
    implementation(libs.io.grpc.grpc.protobuf)
    implementation(libs.io.grpc.grpc.kotlin.stub)
    implementation(libs.com.google.protobuf.protobuf.kotlin)
    implementation(libs.io.grpc.grpc.netty.shaded)
    implementation(libs.org.jetbrains.kotlinx.kotlinx.coroutines.core)
    compileOnly(libs.javax.annotation.javax.annotation.api)
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    compilerOptions { freeCompilerArgs.add("-opt-in=kotlin.RequiresOptIn") }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.35.1"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.81.0"
        }
        create("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:1.5.0:jdk8@jar"
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

sourceSets {
    main {
        proto {
            setSrcDirs(listOf("${rootProject.projectDir}/factstore-proto"))
        }
    }
}

kotlin {
    jvmToolchain(25)
}

