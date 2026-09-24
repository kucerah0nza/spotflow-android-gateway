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
        versionCode = 4
        versionName = "0.1.3"
    }

    // The signing key comes from the environment (CI secrets), never from the repository — anyone holding
    // it can ship an "update" that Android accepts as this app. When set, it signs both debug and release
    // builds, so every CI-built APK is consistently signed and can update the previous one. Without it
    // (local builds, fork PRs) debug builds use the machine's default debug key and release is unsigned.
    val keystoreFile = System.getenv("SPOTFLOW_KEYSTORE_FILE")?.let(::file)?.takeIf { it.exists() }
    val sharedSigning = keystoreFile?.let {
        signingConfigs.create("shared") {
            storeFile = it
            storePassword = System.getenv("SPOTFLOW_KEYSTORE_PASSWORD")
            keyAlias = System.getenv("SPOTFLOW_KEY_ALIAS")
            keyPassword = System.getenv("SPOTFLOW_KEY_PASSWORD")
        }
    }

    buildTypes {
        debug {
            if (sharedSigning != null) signingConfig = sharedSigning
        }
        release {
            isMinifyEnabled = false
            signingConfig = sharedSigning
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
    implementation(libs.material)
}
