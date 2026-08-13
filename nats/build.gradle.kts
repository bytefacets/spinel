import java.time.Instant

plugins {
    `java-library`
    id("com.bytefacets.template_processor") version "0.11.0"
}

apply(plugin = "com.bytefacets.template_processor")

template_processor {
    main {
    }
    test {
    }
    testFixtures {
    }
}

tasks.named("compileJava") {
    dependsOn("create-generated-source-dir", "generate-typed-main-sources")
}

val bytefacetsCollectionsVersion  = extra["bytefacetsCollectionsVersion"] as String
val logbackVersion = extra["logbackVersion"] as String // temporary
val natsVersion = extra["natsVersion"] as String
val nettyVersion = extra["nettyVersion"] as String
val protobufVersion = extra["protobufVersion"] as String
val slfApiVersion = extra["slfApiVersion"] as String // temporary

dependencies {
    implementation("org.slf4j:slf4j-api:${slfApiVersion}") // temporary
    implementation("ch.qos.logback:logback-classic:${logbackVersion}") // temporary

    implementation(project(":spinel"))
    implementation(project(":grpc"))
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")
    implementation("io.nats:jnats:${natsVersion}") // https://mvnrepository.com/artifact/io.nats/jnats
    // https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java
    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")
    implementation("io.netty:netty-transport:${nettyVersion}")

    //testImplementation("io.nats.jnats-test-utils:${natsVersion}")
    testImplementation(testFixtures(project(":spinel")))
}

tasks.named<Jar>("jar") {
    manifest {
        attributes(mapOf(
            "Implementation-Title" to "spinel-nats",
            "Implementation-Version" to project.version.toString(),
            "Implementation-Vendor" to "Byte Facets",
            "Built-By" to System.getProperty("user.name"),
            "Build-Date" to Instant.now().toString()
        ))
    }
}
