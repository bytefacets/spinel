
plugins {
    `java-library`
    id("org.springframework.boot") version "3.4.2"
    id("io.spring.dependency-management") version "1.1.7"
}

// https://docs.spring.io/spring-grpc/reference/
val springGrpcVersion = "0.3.0"
val protobufVersion = "4.29.3";
val springVersion = "7.0.0-M8"
val bytefacetsCollectionsVersion : String by extra
val nettyVersion : String by extra

java {
    withSourcesJar()
    modularity.inferModulePath.set(true)

    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

dependencies {
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")
    implementation(project(":spinel"))
    implementation(project(":grpc"))

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("io.projectreactor.netty:reactor-netty-http:1.3.0-M6") // https://mvnrepository.com/artifact/io.projectreactor.netty/reactor-netty-http
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")
}
