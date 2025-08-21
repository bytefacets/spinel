import com.google.protobuf.gradle.id
plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
    id("com.bytefacets.template_processor") version "0.8.0"
}

apply(plugin = "com.bytefacets.template_processor")

val grpcVersion = "1.70.0"
val protobufVersion = "4.29.3";
val auth0 : String by extra
val bytefacetsCollectionsVersion : String by extra
val log4jVersion : String by extra
val nettyVersion : String by extra

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${protobufVersion}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${grpcVersion}"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc") {
                    option("jakarta_omit")
                    option("@generated=omit")
                }
            }
        }
    }
}

template_processor {
    main {
    }
    test {
    }
    testFixtures {
    }
}

tasks.named("compileJava") {
    dependsOn(
        "generateProto",
        "create-generated-source-dir",
        "generate-typed-main-sources",
        "generate-typed-test-sources",
        "generate-typed-testFixtures-sources")
}

dependencies {
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")
    implementation(project(":spinel"))
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")
//    implementation("io.grpc:grpc-netty-shaded:${grpcVersion}")
    implementation("io.grpc:grpc-netty:${grpcVersion}")
    implementation("io.netty:netty-transport:${nettyVersion}")
    implementation("com.auth0:java-jwt:${auth0}")

    // https://mvnrepository.com/artifact/io.grpc/protoc-gen-grpc-java
    implementation("io.grpc:protoc-gen-grpc-java:${grpcVersion}")

    // https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")

    testImplementation("io.grpc:grpc-inprocess:${grpcVersion}")
    testImplementation(testFixtures(project(":spinel")))
}
