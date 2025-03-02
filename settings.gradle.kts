rootProject.name = "com.bytefacets"

pluginManagement {
    repositories {
        mavenLocal()
        gradlePluginPortal()
        repositories {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/bytefacets/type-template-processor")
                credentials {
                    username = System.getenv("USERNAME") ?: providers.gradleProperty("gpr.user").orElse("no-username").get()
                    password = System.getenv("TOKEN") ?: providers.gradleProperty("gpr.key").orElse("no-token").get()
                }
            }
        }
    }
}

include("facets")
include("examples")
