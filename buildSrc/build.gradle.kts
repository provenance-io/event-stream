plugins {
    `kotlin-dsl`
}

val gradleNexusVersion = "1.1.0"
val kotlinVersion = "1.7.10"
val ktLintVersion = "10.3.0"
val dokkaVersion = kotlinVersion

repositories {
    gradlePluginPortal()
}

ext {
    set("kotlinVersion", kotlinVersion)
    set("ktLintVersion", ktLintVersion)
    set("dokkaVersion", dokkaVersion)
}

dependencies {
    implementation("io.github.gradle-nexus", "publish-plugin", gradleNexusVersion)
    implementation("org.jetbrains.kotlin", "kotlin-gradle-plugin", kotlinVersion)
    implementation("org.jlleitschuh.gradle", "ktlint-gradle", ktLintVersion)
    implementation("org.jetbrains.dokka", "dokka-gradle-plugin", dokkaVersion)
}
