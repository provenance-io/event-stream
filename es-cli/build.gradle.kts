plugins {
    kotlin("jvm")

    application
    id("core-config")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(projects.esApi)
    implementation(projects.esApiModel)
    implementation(projects.esCore)
    implementation(projects.esKafka)

    implementation(libs.bundles.logback)
    implementation(libs.bundles.moshi)

    implementation(libs.kafka)
    implementation(libs.kotlinx.cli)
    implementation(libs.okhttp.client)
    implementation(libs.bundles.grpc)
    implementation(libs.provenance.protos)
}

application {
    applicationName = "event-stream"
    mainClass.set("io.provenance.eventstream.NuMainKt")
}
