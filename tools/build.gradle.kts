plugins {
    java
    application
}

application {
    mainClass.set("com.bytefacets.spinel.tools.Main")
}

val auth0  = extra["auth0"] as String
val bytefacetsCollectionsVersion  = extra["bytefacetsCollectionsVersion"] as String
val grpcVersion  = extra["grpcVersion"] as String
val logbackVersion = extra["logbackVersion"] as String
val nettyVersion  = extra["nettyVersion"] as String
val protobufVersion  = extra["protobufVersion"] as String
var picocliVersion = "4.7.7"
var jansiVersion  = "4.3.1"
val slfApiVersion = extra["slfApiVersion"] as String

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

    // https://mvnrepository.com/artifact/org.jline/jansi
    implementation("org.jline:jansi:${jansiVersion}")
}
