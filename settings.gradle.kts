rootProject.name = "spinel"

pluginManagement {
    repositories {
        mavenLocal()
        gradlePluginPortal()
    }
}

include("spinel")
include("examples")
include("grpc")
include("spring-examples")
