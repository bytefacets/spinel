import org.jreleaser.model.Active

// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
plugins {
    id("org.jreleaser")
}

jreleaser {
    deploy {
//        environment {
//            variables.set(file("$rootDir/jreleaser-config.yml"))
//        }
        signing {
            active.set(Active.ALWAYS)
            armored.set(true)
        }

        deploy {
            maven {
                mavenCentral {
                    create("sonatype") {
                        active.set(Active.ALWAYS)
                        url.set("https://central.sonatype.com/api/v1/publisher")
                        stagingRepository("collections/build/sonatype/BuildMaven-java-bundle")
                    }
                }
            }
        }
    }
}
