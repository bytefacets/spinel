plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

val jvmTargetVer = JavaLanguageVersion.of(25)

java {
    toolchain.languageVersion.set(jvmTargetVer)
}

kotlin {
    jvmToolchain {
        (this as JavaToolchainSpec).languageVersion.set(jvmTargetVer)
    }
}

dependencies {
    implementation("io.github.gradle-nexus:publish-plugin:1.3.0")
    implementation("com.tddworks.central-portal-publisher:com.tddworks.central-portal-publisher.gradle.plugin:0.0.5")
    implementation("com.gradle.publish:plugin-publish-plugin:1.2.1")
}