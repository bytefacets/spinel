
plugins {
    `java-library`
    id("org.springframework.boot") version "4.1.0" // https://plugins.gradle.org/plugin/org.springframework.boot
    id("io.spring.dependency-management") version "1.1.7" // https://plugins.gradle.org/plugin/io.spring.dependency-management
}

val bytefacetsCollectionsVersion : String by extra
val protobufVersion: String by extra
val nettyVersion : String by extra

java {
    withSourcesJar()
    modularity.inferModulePath.set(true)
}

configurations.spotbugs {
    resolutionStrategy {
        force("com.github.spotbugs:spotbugs-annotations:4.10.3")
        force("org.apache.commons:commons-lang3:3.17.0")
    }
}

dependencies {
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")
    implementation(project(":spinel"))
    implementation(project(":grpc"))

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}") // https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java
}
