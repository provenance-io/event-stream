plugins {
    kotlin("jvm")
    kotlin("kapt")
    id("core-config")
    id("org.openapi.generator") version(Versions.Plugins.OpenAPI)
}

dependencies {
    kapt(libs.moshi.kotlin.codegen)

    implementation(libs.bundles.grpc)
    implementation(libs.bundles.hoplite)
    implementation(libs.bundles.scarlet)

    implementation(libs.arrow)
    implementation(libs.commons.lang)
    implementation(libs.json)

    implementation(libs.bundles.grpc)
    implementation(libs.provenance.protos)
}

sourceSets {
    main {
        java.srcDirs(
            "$projectDir/api-model/src/main/kotlin",
            "$buildDir/generated/src/main/kotlin",
        )
    }
    test {
        java.srcDir("$projectDir/api-model/src/test/kotlin")
    }
}

kapt {
    correctErrorTypes = true
}

project.afterEvaluate {
    // Force generation of the API and models based on the
    tasks.get("kaptGenerateStubsKotlin").dependsOn("generateTendermintAPI")
}

val tendermintOpenApiYaml = "$rootDir/es-api-model/src/main/resources/tendermint-v0.34.12-rpc-openapi-FIXED.yaml"

/**
 * See the following links for information about generating models from an OpenAPI spec:
 * - https://github.com/OpenAPITools/openapi-generator/tree/master/modules/openapi-generator-gradle-plugin
 * - https://github.com/OpenAPITools/openapi-generator/blob/master/docs/global-properties.md
 * - https://github.com/OpenAPITools/openapi-generator/blob/master/docs/generators/kotlin.md
 */
tasks.register<org.openapitools.generator.gradle.plugin.tasks.GenerateTask>("generateTendermintAPI") {
    generatorName.set("kotlin")
    verbose.set(false)
    validateSpec.set(true)
    inputSpec.set(tendermintOpenApiYaml)
    outputDir.set("$buildDir/generated")
    packageName.set("io.provenance.eventstream.stream")
    modelPackage.set("io.provenance.eventstream.stream.models")
    library.set("jvm-okhttp4")
    configOptions.set(
        mapOf(
            "artifactId" to "tendermint-api",
            "dateLibrary" to "java8",
            "moshiCodeGen" to "true",
            "modelMutable" to "false",
            "serializableModel" to "true",
            "useCoroutines" to "true"
        )
    )
}
