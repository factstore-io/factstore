// What every FactStore module shares: Kotlin on the JVM, one Java version, and JUnit 5.

plugins {
    id("org.jetbrains.kotlin.jvm")
}

kotlin {
    // The toolchain sets the source and target level of both Kotlin and Java, and builds with a
    // JDK 25 whatever JDK runs Gradle.
    jvmToolchain(25)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
