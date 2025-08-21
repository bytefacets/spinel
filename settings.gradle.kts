rootProject.name = "spinel"

pluginManagement {
    repositories {
        mavenLocal()
        gradlePluginPortal()
        repositories {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/bytefacets/type-template-processor")
                credentials {
                    username = System.getenv("GITHUB_ACTOR") ?: providers.gradleProperty("gpr.user").orElse("no-username").get()
                    password = System.getenv("GITHUB_TOKEN") ?: providers.gradleProperty("gpr.key").orElse("no-token").get()
                }
            }
        }
    }
}

include("spinel")
include("examples")
include("grpc")
