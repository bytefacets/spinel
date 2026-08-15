plugins {
    java
}

tasks.withType<PublishToMavenRepository>().configureEach { enabled = false }
tasks.withType<PublishToMavenLocal>().configureEach { enabled = false }

val auth0 = extra["auth0"] as String
val bytefacetsCollectionsVersion = extra["bytefacetsCollectionsVersion"] as String
val grpcVersion = extra["grpcVersion"] as String
val logbackVersion = extra["logbackVersion"] as String
val natsVersion = extra["natsVersion"] as String
val nettyVersion = extra["nettyVersion"] as String
val slfApiVersion = extra["slfApiVersion"] as String

dependencies {
    implementation(project(":spinel"))
    implementation(project(":grpc"))
    implementation(project(":nats"))
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")

    implementation("org.slf4j:slf4j-api:${slfApiVersion}")
    implementation("ch.qos.logback:logback-classic:${logbackVersion}")

    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")
    implementation("io.grpc:grpc-netty:${grpcVersion}")
    implementation("io.netty:netty-transport:${nettyVersion}")
    implementation("com.auth0:java-jwt:${auth0}")

    implementation("io.nats:jnats:${natsVersion}") // https://mvnrepository.com/artifact/io.nats/jnats
    implementation("net.datafaker:datafaker:2.7.0") // https://mvnrepository.com/artifact/net.datafaker/datafaker
}
