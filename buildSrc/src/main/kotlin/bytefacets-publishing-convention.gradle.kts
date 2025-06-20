// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
plugins {
    java
    signing
    `maven-publish`
}

java {
    withSourcesJar()
    withJavadocJar()
}

tasks.javadoc {
    if (JavaVersion.current().isJava9Compatible) {
        (options as StandardJavadocDocletOptions).apply {
            addBooleanOption("html5", true)
            // Why -quiet? See: https://github.com/gradle/gradle/issues/2354
            addStringOption("Xwerror", "-quiet")
        }
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/bytefacets/${rootProject.name}")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }

    group = "com.bytefacets"

    publications {
        create<MavenPublication>("maven-java") {
            from(components["java"])

            val prependRootName = rootProject.name != project.name
            if (prependRootName) {
                artifactId = "${rootProject.name}-${project.name}"
            }
            artifactId = "bytefacets-${artifactId}"

            pom {
                name.set("${project.group}:${artifactId}")

                if (prependRootName) {
                    description.set("${rootProject.name} ${project.name} library".replace("-", " "))
                } else {
                    description.set("${project.name} library".replace("-", " "))
                }

                url.set("https://www.bytefacets.com")
                inceptionYear.set("2024")

                licenses {
                    license {
                        name.set("MIT License")
                    }
                }

                organization {
                    name.set("Byte Facets")
                    url.set("https://www.bytefacets.com")
                }

                developers {
                    developer {
                        organization.set("Byte Facets")
                        organizationUrl.set("https://www.bytefacets.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/bytefacets/${rootProject.name}.git")
                    developerConnection.set("scm:git:ssh://github.com/bytefacets/${rootProject.name}.git")
                    url.set("https://github.com/bytefacets/${rootProject.name}")
                }

                issueManagement {
                    system.set("GitHub issues")
                    url.set("https://github.com/bytefacets/${rootProject.name}/issues")
                }
            }
        }
    }
}

signing {
    setRequired {
        !project.version.toString().endsWith("-SNAPSHOT")
                && !project.hasProperty("skipSigning")
    }

    if (System.getenv("GPG_PRIVATE_KEY") != null && System.getenv("GPG_PASSPHRASE") != null) {
        useInMemoryPgpKeys(System.getenv("GPG_PRIVATE_KEY").toString(),
            System.getenv("GPG_PASSPHRASE").toString())
    }

    sign(publishing.publications["maven-java"])
}