import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.binary.compatibility.validator)
    `maven-publish`
}

// One version for the library and the demo app, set in the root gradle.properties.
val spotflowVersion: String = providers.gradleProperty("spotflowVersion").get()

android {
    namespace = "io.spotflow.ble"
    compileSdk = 35

    defaultConfig {
        minSdk = 26
        consumerProguardFiles("consumer-rules.pro")
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro",
            )
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }

    testOptions {
        unitTests.isReturnDefaultValues = true
        unitTests.isIncludeAndroidResources = true // required by Robolectric
    }
}

kotlin {
    // Everything public is public on purpose: a declaration without a visibility modifier doesn't compile.
    explicitApi()
    // Host apps on Kotlin 2.1 or newer can use the library. A compiler reads metadata at most one
    // version ahead of its own, and the metadata version follows the language version, so building with
    // the latest language version would force every host onto the same Kotlin. The API version keeps the
    // library off standard-library APIs newer than 2.1, and the published POM asks for that stdlib.
    // The floor is 2.1, not lower, because of kotlinx-coroutines: 1.11 is built with Kotlin 2.2, which a
    // 2.0 compiler cannot read. Raise these together with any dependency built with a newer Kotlin.
    coreLibrariesVersion = "2.1.21"
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        languageVersion.set(KotlinVersion.KOTLIN_2_1)
        apiVersion.set(KotlinVersion.KOTLIN_2_1)
    }
}

dependencies {
    // `api`: StateFlow is part of the public API (SpotflowGateway.devices, …).
    api(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.androidx.core.ktx)
    // An implementation detail: no MQTT type appears in the public API.
    implementation(libs.hivemq.mqtt.client)

    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.mockk)
    testImplementation(libs.robolectric)
    testImplementation(libs.androidx.test.core)
}

publishing {
    publications {
        register<MavenPublication>("release") {
            groupId = "io.spotflow"
            artifactId = "spotflow-ble-gateway"
            version = spotflowVersion

            afterEvaluate {
                from(components["release"])
            }

            pom {
                name.set("Spotflow BLE Gateway")
                description.set("Android library that relays Spotflow BLE device diagnostics to the Spotflow cloud.")
                url.set("https://spotflow.io")
                licenses {
                    license {
                        // The same licence as the Spotflow Device SDK; see LICENSE.md in the repository.
                        name.set("Business Source License 1.1")
                        url.set("https://github.com/kucerah0nza/spotflow-android-gateway/blob/main/LICENSE.md")
                        distribution.set("repo")
                    }
                }
            }
        }
    }
    repositories {
        // A Maven repository in a directory. The release workflow zips it and attaches it to the GitHub
        // Release, so integrators get the AAR *with* its POM — and so its dependencies — rather than a
        // bare .aar. Not published to Maven Central yet.
        maven {
            name = "dist"
            url = uri(rootProject.layout.buildDirectory.dir("maven-repo"))
        }
    }
}
