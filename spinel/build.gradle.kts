import java.time.Instant

plugins {
    `java-library`
    id("java-test-fixtures")
    id("com.bytefacets.template_processor") version "0.10.0"
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
            "BoolIndexedTableBuilder.java",
            "BoolIndexedStructTable.java",
            "BoolIndexedStructTableBuilder.java",
            "BoolRowInterner.java",
            "BoolTableHandle.java"))
    }
    test {
        excludedFiles.set(listOf(
            "BoolIndexedTableTest.java",
            "BoolIndexedStructTableTest.java",
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
val bytefacetsCollectionsVersion : String by extra

dependencies {
    implementation("com.bytefacets:bytefacets-collections:${bytefacetsCollectionsVersion}")
    implementation("org.javassist:javassist:3.30.2-GA") // https://mvnrepository.com/artifact/org.javassist/javassist
    testFixturesImplementation("com.bytefacets:bytefacets-collections:0.3.0")
    implementation("org.apache.commons:commons-jexl3:3.5.0") // https://mvnrepository.com/artifact/org.apache.commons/commons-jexl3
}

tasks.named<Jar>("jar") {
    manifest {
        attributes(mapOf(
            "Implementation-Title" to "spinel",
            "Implementation-Version" to project.version.toString(),
            "Implementation-Vendor" to "Byte Facets",
            "Built-By" to System.getProperty("user.name"),
            "Build-Date" to Instant.now().toString()
        ))
    }
}
