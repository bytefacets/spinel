plugins {
    java
    `bytefacets-publishing-convention` apply false
    `bytefacets-central-portal-publishing-convention`
    id("pl.allegro.tech.build.axion-release") version "1.18.18" // https://plugins.gradle.org/plugin/pl.allegro.tech.build.axion-release
    id("com.github.spotbugs") version "6.0.25"                  // https://mvnrepository.com/artifact/com.github.spotbugs/spotbugs-gradle-plugin
    id("com.diffplug.spotless") version "8.8.0"                 // https://mvnrepository.com/artifact/com.diffplug.spotless/spotless-plugin-gradle
}

gradle.startParameter.showStacktrace = ShowStacktrace.ALWAYS
group = "com.bytefacets"

apply(plugin = "com.tddworks.central-portal-publisher")

project.version = System.getenv("GIT_TAG") ?: "0.0.1-SNAPSHOT"
System.out.printf("VERSION '%s'%n", version)

allprojects {
    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")

    java {
        withSourcesJar()
        modularity.inferModulePath.set(true)

        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    repositories {
        mavenCentral()
        google()
        gradlePluginPortal()
        repositories {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/bytefacets/collections")
                credentials {
                    username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
                    password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
                }
            }
        }
    }

    tasks.register("create-generated-source-dir") {
        doLast {
            mkdir(layout.projectDirectory.dir("src/main/generated"))
            mkdir(layout.projectDirectory.dir("src/test/generated"))
            mkdir(layout.projectDirectory.dir("src/testFixtures/generated"))
        }
    }

    sourceSets {
        named("main") {
            java.srcDir(layout.projectDirectory.dir("src/main/generated"))
        }
        named("test") {
            java.srcDir(layout.projectDirectory.dir("src/test/generated"))
        }
    }
}

subprojects {
    apply(plugin = "maven-publish")

    if(project.name == "spinel" || project.name == "grpc") {
        apply(plugin = "bytefacets-publishing-convention")
    }

    project.version = project.parent?.version!!

    extra.apply {
        set("auth0", "4.4.0")
        set("bytefacetsCollectionsVersion", "0.7.0")
        set("findbugsVersion", "4.7.3")
        set("grpcVersion", "1.82.2")          // https://mvnrepository.com/artifact/io.grpc/protoc-gen-grpc-java
        set("logbackVersion", "1.5.38")       // https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
        set("natsVersion", "2.26.0")          // https://mvnrepository.com/artifact/io.nats/jnats
        set("nettyVersion", "4.2.16.Final")   // https://mvnrepository.com/artifact/io.netty/netty-all
        set("protobufVersion", "4.36.0-RC1")  // https://mvnrepository.com/artifact/com.google.protobuf/protoc
        set("slfApiVersion", "2.0.18")        // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
        set("spotbugsVersion", "4.10.3")      // https://mvnrepository.com/artifact/com.github.spotbugs/spotbugs-annotations
    }

    val spotbugsVersion: String by extra
    val findbugsVersion: String by extra
    val logbackVersion: String by extra
    val slfApiVersion: String by extra
    val junitVersion = "5.13.4"
    val hamcrestVersion = "2.2"
    val mockitoVersion = "5.23.0" // https://mvnrepository.com/artifact/org.mockito/mockito-core
    val jakartaAnnotationVersion = "2.1.1"

    val mockitoAgent = configurations.create("mockitoAgent")
    dependencies {
        spotbugs("com.github.spotbugs:spotbugs:${spotbugsVersion}")
        compileOnly("jakarta.annotation:jakarta.annotation-api:${jakartaAnnotationVersion}") // for the module-info resolution
        compileOnly("com.github.spotbugs:spotbugs-annotations:${findbugsVersion}")
        compileOnly("org.slf4j:slf4j-api:${slfApiVersion}")

        testImplementation("org.slf4j:slf4j-api:${slfApiVersion}")
        testImplementation("ch.qos.logback:logback-classic:${logbackVersion}")

        testImplementation("org.mockito:mockito-junit-jupiter:${mockitoVersion}")
        testImplementation("org.hamcrest:hamcrest:${hamcrestVersion}")

        testImplementation(platform("org.junit:junit-bom:${junitVersion}"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testImplementation("org.junit.jupiter:junit-jupiter-params")

        // Explicitly align the launcher with the engine version from the BOM.
        // Without this, Gradle auto-adds junit-platform-launcher at a version
        // that may not match junit-platform-engine, causing:
        // "OutputDirectoryProvider not available; probably due to unaligned versions..."
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")

        testCompileOnly("jakarta.annotation:jakarta.annotation-api:${jakartaAnnotationVersion}") // for the module-info resolution
        testCompileOnly("com.github.spotbugs:spotbugs-annotations:${findbugsVersion}")
        mockitoAgent("org.mockito:mockito-core:${mockitoVersion}") {
            isTransitive = false
        }
    }

    tasks.withType<Javadoc> {
        options.encoding = "UTF-8"
        (options as StandardJavadocDocletOptions).apply {
            addStringOption("Xdoclint:none", "-quiet")
        }
        isFailOnError = false
    }

    tasks.compileJava {
        options.compilerArgs.add("-Xlint:all,-serial,-requires-automatic,-requires-transitive-automatic,-module")
        options.compilerArgs.add("-Werror")
    }

    tasks.test {
        useJUnitPlatform()
        setForkEvery(1)
        jvmArgs("-javaagent:${mockitoAgent.asPath}")
        maxParallelForks = 4
        testLogging {
            showStandardStreams = true
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showCauses = true
            showExceptions = true
            showStackTraces = true
        }
    }

    spotless {
        java {
            target("src/main/java/**/*.java", "src/test/java/**/*.java")
            googleJavaFormat("1.25.2").aosp()
            indentWithSpaces()
            importOrder()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
            toggleOffOn("formatting:off", "formatting:on")
            licenseHeaderFile(
                rootProject.file("config/spotless/spdx-header.txt"),
                "package "
            )
        }
    }

    spotbugs {
        showProgress.set(false)
        excludeFilter.set(rootProject.file("config/spotbugs/exclude.xml"))
        if(file("config/spotbugs/exclude.xml").exists()) {
            excludeFilter.set(file("config/spotbugs/exclude.xml"))
        }

        tasks.spotbugsMain {
            reports.create("html") {
                enabled = true
                setStylesheet("fancy-hist.xsl")
            }
        }
        tasks.spotbugsTest {
            reports.create("html") {
                enabled = true
                setStylesheet("fancy-hist.xsl")
            }
        }
    }

    tasks.jar {

    }

    tasks.register("pre") {
        dependsOn("spotlessCheck", "spotlessApply")
    }

    tasks.register("preClean") {
        dependsOn("clean", "pre")
    }

    tasks.register("checkstyle") {
        dependsOn("checkstyleMain", "checkstyleTest")
    }

    tasks.register("spotbugs") {
        dependsOn("spotbugsMain", "spotbugsTest")
    }

    tasks.register("static") {
        dependsOn("checkstyle", "spotbugs")
    }

    tasks.register<DependencyReportTask>("allDeps")
}
