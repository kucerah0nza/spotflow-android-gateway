plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
}

android {
    namespace = "io.spotflow.gateway.demo"
    compileSdk = 35

    defaultConfig {
        applicationId = "io.spotflow.gateway.demo"
        minSdk = 26
        targetSdk = 35
        versionCode = 3
        versionName = "0.1.2"
    }

    signingConfigs {
        // A stable, shared signing key committed to the repo so every build — local and CI — produces
        // consistently-signed, updatable APKs. This is a development key (its password is not secret); a
        // real production/Play signing key would live in CI secrets instead.
        create("shared") {
            storeFile = file("signing/spotflow-shared.keystore")
            storePassword = "spotflow"
            keyAlias = "spotflow"
            keyPassword = "spotflow"
        }
    }

    buildTypes {
        debug {
            signingConfig = signingConfigs.getByName("shared")
        }
        release {
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("shared")
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

    kotlinOptions {
        jvmTarget = "17"
    }

    buildFeatures {
        viewBinding = true
    }

    packaging {
        resources {
            // HiveMQ pulls in several Netty jars that ship colliding META-INF entries.
            excludes += setOf(
                "META-INF/INDEX.LIST",
                "META-INF/DEPENDENCIES",
                "META-INF/LICENSE",
                "META-INF/LICENSE.txt",
                "META-INF/NOTICE",
                "META-INF/NOTICE.txt",
                "META-INF/*.kotlin_module",
            )
            pickFirsts += setOf(
                "META-INF/io.netty.versions.properties",
                "META-INF/native-image/**",
            )
        }
    }
}

dependencies {
    implementation(project(":spotflow-ble-gateway"))
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.appcompat)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.security.crypto)
    implementation(libs.material)
}
