plugins {
    java
    id("com.bytefacets.template_processor") version "0.11.0"
}

template_processor {
    main { }
    test { }
    testFixtures { }
}

tasks.named("compileJava") {
    dependsOn(
        "create-generated-source-dir",
        "generate-typed-main-sources",
        "generate-typed-test-sources")
}

val auth0 = extra["auth0"] as String
val bytefacetsCollectionsVersion = extra["bytefacetsCollectionsVersion"] as String
val grpcVersion = extra["grpcVersion"] as String
val logbackVersion = extra["logbackVersion"] as String
val natsVersion = extra["natsVersion"] as String
val nettyVersion = extra["nettyVersion"] as String
val slfApiVersion = extra["slfApiVersion"] as String
val vaadinVersion = "24.10.8"

dependencies {
    implementation(project(":spinel"))
    implementation(project(":grpc"))

    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")
    implementation("com.vaadin:vaadin-core:${vaadinVersion}")
}
