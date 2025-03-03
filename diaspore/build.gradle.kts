plugins {
    `java-library`
    id("java-test-fixtures")
    id("com.bytefacets.template_processor") version "0.8.0"
}

apply(plugin = "com.bytefacets.template_processor")

sourceSets {
    named("testFixtures") {
        java.srcDir(layout.projectDirectory.dir("src/testFixtures/generated"))
    }
}

template_processor {
    main {
        excludedFiles.set(listOf(
            "BoolIndexedTable.java",
            "BoolTableHandle.java",
            "BoolIndexedTableBuilder.java",
            "BoolRowInterner.java"))
    }
    test {
        excludedFiles.set(listOf(
            "BoolIndexedTableTest.java",
            "BoolIndexedSetTest.java"))
    }
    testFixtures {
        excludedFiles.set(listOf("BoolTableHandle.java"))
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
    api("com.bytefacets:collections:0.1.0")
    testImplementation(testFixtures(project(":diaspore")))
}
