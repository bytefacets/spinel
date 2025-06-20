plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

val jvmTargetVer = JavaLanguageVersion.of(17)

java {
    toolchain.languageVersion.set(jvmTargetVer)
}

kotlin {
    jvmToolchain {
        (this as JavaToolchainSpec).languageVersion.set(jvmTargetVer)
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "$jvmTargetVer"
    }
}

dependencies {
    implementation("io.github.gradle-nexus:publish-plugin:1.3.0")
    implementation("org.jreleaser:org.jreleaser.gradle.plugin:1.18.0") // https://plugins.gradle.org/plugin/org.jreleaser
    implementation("com.gradle.publish:plugin-publish-plugin:1.2.1")
}