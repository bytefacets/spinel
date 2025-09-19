plugins {
    java
    application
}

application {
    mainClass.set("com.bytefacets.spinel.tools.Main")
}

val auth0 : String by extra
val bytefacetsCollectionsVersion : String by extra
val grpcVersion : String by extra
val log4jVersion : String by extra
val logbackVersion: String by extra
val nettyVersion : String by extra
val protobufVersion : String by extra
var picocliVersion = "4.7.7"
val slfApiVersion: String by extra

dependencies {
    implementation(project(":spinel"))
    implementation(project(":grpc"))
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")

    implementation("org.slf4j:slf4j-api:${slfApiVersion}")
    implementation("ch.qos.logback:logback-classic:${logbackVersion}")

    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")
    implementation("io.grpc:grpc-netty:${grpcVersion}")
    implementation("io.netty:netty-transport:${nettyVersion}")
    implementation("com.auth0:java-jwt:${auth0}")
    // https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")

    // https://mvnrepository.com/artifact/info.picocli/picocli
    implementation("info.picocli:picocli:${picocliVersion}")

    // https://mvnrepository.com/artifact/org.fusesource.jansi/jansi
    implementation("org.fusesource.jansi:jansi:2.4.2")
}
