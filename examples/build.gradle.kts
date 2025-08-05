plugins {
    java
}

tasks.withType<PublishToMavenRepository>().configureEach { enabled = false }
tasks.withType<PublishToMavenLocal>().configureEach { enabled = false }

val auth0: String by extra
val grpcVersion = "1.70.0"
val slfApiVersion: String by extra
val logbackVersion: String by extra
val nettyVersion: String by extra
val bytefacetsCollectionsVersion: String by extra

dependencies {
    implementation(project(":diaspore"))
    implementation(project(":grpc"))
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")

    implementation("org.slf4j:slf4j-api:${slfApiVersion}")
    implementation("ch.qos.logback:logback-classic:${logbackVersion}")

    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")
    implementation("io.grpc:grpc-netty:${grpcVersion}")
    implementation("io.netty:netty-transport:${nettyVersion}")
    implementation("com.auth0:java-jwt:${auth0}")
}
