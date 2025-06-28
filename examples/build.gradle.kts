plugins {
    java
}

tasks.withType<PublishToMavenRepository>().configureEach { enabled = false }
tasks.withType<PublishToMavenLocal>().configureEach { enabled = false }

dependencies {
    implementation(project(":diaspore"))
}
