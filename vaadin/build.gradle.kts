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

val auth0: String by extra
val bytefacetsCollectionsVersion: String by extra
val grpcVersion: String by extra
val logbackVersion: String by extra
val natsVersion: String by extra
val nettyVersion: String by extra
val slfApiVersion: String by extra
val vaadinVersion = "24.8.8"

dependencies {
    implementation(project(":spinel"))
    implementation(project(":grpc"))

    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")
    implementation("com.vaadin:vaadin-core:${vaadinVersion}")
}
