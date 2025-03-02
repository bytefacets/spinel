plugins {
    `java-library`
    id("com.bytefacets.template_processor") version "0.7.0"
}

apply(plugin = "com.bytefacets.template_processor")

template_processor {
    main {
        excludedFiles.set(listOf())
    }
    test {
        excludedFiles.set(listOf())
    }
    testFixtures {
        excludedFiles.set(listOf())
    }
}

tasks.named("compileJava") {
    dependsOn(
        "create-generated-source-dir",
        "generate-typed-main-sources",
        "generate-typed-test-sources",
        "generate-typed-testFixtures-sources")
}

dependencies {
    implementation("com.bytefacets:collections:0.1.0")
}
