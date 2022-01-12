import util.extensions.javadocJar

plugins {
    kotlin("jvm")
    id("java-library")
    id("com.google.protobuf") version Plugins.Protobuf
    idea
    id("maven-publish")
    id("core-config")
    id("with-linter")
    id("with-test-fixtures")
    id("generate-docs")
}

group = rootProject.group
version = rootProject.version

dependencies {
    api(project(":api"))
    api(project(":api-model"))

    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin", "kotlin-stdlib")
    implementation("org.jetbrains.kotlin", "kotlin-reflect")
    implementation("org.jetbrains.kotlinx", "kotlinx-datetime", Versions.Kotlinx.DateTime)
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", Versions.Kotlinx.Core)
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-jdk8", Versions.Kotlinx.Core)
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-reactive", Versions.Kotlinx.Core)
    implementation("io.arrow-kt", "arrow-core", Versions.Arrow)
    implementation("org.apache.commons", "commons-lang3", Versions.ApacheCommons.Lang3)
    implementation("com.tinder.scarlet", "scarlet", Versions.Scarlet)
    implementation("com.tinder.scarlet", "stream-adapter-coroutines", Versions.Scarlet)
    implementation("com.tinder.scarlet", "websocket-okhttp", Versions.Scarlet)
    implementation("com.tinder.scarlet", "message-adapter-moshi", Versions.Scarlet)
    implementation("io.grpc", "grpc-alts", Versions.GRPC)
    implementation("io.grpc", "grpc-netty", Versions.GRPC)
    implementation("io.grpc", "grpc-protobuf", Versions.GRPC)
    implementation("io.grpc", "grpc-stub", Versions.GRPC)
    implementation("io.provenance.protobuf", "pb-proto-java", Versions.Provenance)
    runtimeOnly("ch.qos.logback", "logback-classic", Versions.LogBack)
    implementation("io.github.microutils", "kotlin-logging-jvm", Versions.KotlinLogging)
    implementation("com.squareup.moshi", "moshi-kotlin-codegen", Versions.Moshi)
    kapt("com.squareup.moshi:moshi-kotlin-codegen:${Versions.Moshi}")
    implementation("com.sksamuel.hoplite", "hoplite-core", Versions.Hoplite)
    implementation("com.sksamuel.hoplite", "hoplite-yaml", Versions.Hoplite)
    implementation("org.json", "json", Versions.JSON)
}

kapt {
    correctErrorTypes = true
}

publishing {
    repositories {}

    publications {
        create<MavenPublication>("maven") {
            groupId = rootProject.group.toString()
            artifactId = rootProject.name
            version = rootProject.version.toString()
            /* Skip outputting the test-fixtures jar:
            from(
                util.filterTestFixturesFromComponents(
                    configurations,
                    components["java"] as AdhocComponentWithVariants
                )
            )
            */
            from(components["java"])
            artifact(project.javadocJar())
        }
    }
}