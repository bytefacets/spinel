import java.io.ByteArrayOutputStream
gradle.startParameter.showStacktrace = ShowStacktrace.ALWAYS

plugins {
    java
    id("com.github.spotbugs") version "6.0.25"                  // https://mvnrepository.com/artifact/com.github.spotbugs/spotbugs-gradle-plugin
    id("com.diffplug.spotless") version "6.19.0"                 // https://mvnrepository.com/artifact/com.diffplug.spotless/spotless-plugin-gradle
}

group = "com.bytefacets"

fun getVersionFromGit(): String {
    val stdout = ByteArrayOutputStream()
    exec {
        commandLine("git", "describe", "--tags", "--always", "--dirty")
        standardOutput = stdout
    }
    val gitVersion = stdout.toString()
    val base = if(gitVersion.contains('-')) gitVersion.substring(0, gitVersion.indexOf('-')) else gitVersion
    val isSnapshot = gitVersion.contains("dirty")
    val snapshot = if (isSnapshot) "-SNAPSHOT" else ""
    return base + snapshot
}

version = getVersionFromGit()
System.out.printf("VERSION '%s'%n", version)

allprojects {
    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")

    java {
        withSourcesJar()

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

    project.version = project.parent?.version!!

    extra.apply {
        set("guavaVersion", "31.0.1-jre")
        set("findbugsVersion", "4.7.3")
        set("spotbugsVersion", "4.8.6")
    }

    val spotbugsVersion: String by extra
    val findbugsVersion: String by extra
    val guavaVersion: String by extra
    val junitVersion = "5.7.0"
    val hamcrestVersion = "2.2"
    val mockitoVersion = "3.12.4"
    val junitPioneerVersion = "0.9.0"

    val mockitoAgent = configurations.create("mockitoAgent")
    dependencies {
        spotbugs("com.github.spotbugs:spotbugs:${spotbugsVersion}")
        implementation("com.github.spotbugs:spotbugs-annotations:${findbugsVersion}")

        testImplementation("org.mockito:mockito-junit-jupiter:${mockitoVersion}")
        testImplementation("org.hamcrest:hamcrest:${hamcrestVersion}")
        testImplementation("com.google.guava:guava-testlib:${guavaVersion}")
        testImplementation("org.junit.jupiter:junit-jupiter-api:${junitVersion}")
        testImplementation("org.junit.jupiter:junit-jupiter-params:${junitVersion}")
        testImplementation("org.junit-pioneer:junit-pioneer:${junitPioneerVersion}")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${junitVersion}")
        mockitoAgent("org.mockito:mockito-core:${mockitoVersion}") {
            isTransitive = false
        }
    }

    tasks.compileJava {
        options.compilerArgs.add("-Xlint:all,-serial,-requires-automatic,-requires-transitive-automatic,-module")
        options.compilerArgs.add("-Werror")
    }

    tasks.test {
        useJUnitPlatform()
        setForkEvery(1)
        //jvmArgs("-javaagent:${mockitoAgent.asPath}")
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
        }
    }

    spotbugs {
        showProgress.set(true)
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
        archiveBaseName.set(group)
    }

    configure<PublishingExtension> {
        publications {
            repositories {
                maven {
                    name = "GitHubPackages"
                    url = uri("https://maven.pkg.github.com/bytefacets/diaspore")
                    credentials {
                        username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
                        password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
                    }
                }
            }
            create<MavenPublication>("gpr") {
                from(components["java"])
                groupId = "com.bytefacets"
                artifactId = "bytefacets-${project.name}"
            }
        }
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
