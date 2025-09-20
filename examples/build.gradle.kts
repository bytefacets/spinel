plugins {
    java
}

tasks.withType<PublishToMavenRepository>().configureEach { enabled = false }
tasks.withType<PublishToMavenLocal>().configureEach { enabled = false }

val auth0: String by extra
val bytefacetsCollectionsVersion: String by extra
val grpcVersion = "1.70.0"
val logbackVersion: String by extra
val natsVersion: String by extra
val nettyVersion: String by extra
val slfApiVersion: String by extra

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
    implementation("net.datafaker:datafaker:2.5.0") // https://mvnrepository.com/artifact/net.datafaker/datafaker
}
