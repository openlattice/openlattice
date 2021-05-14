plugins {
    kotlin("jvm") version "1.4.31"
    `maven-publish`
    signing
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "11"
    }
}

apply("./openlattice.gradle")
