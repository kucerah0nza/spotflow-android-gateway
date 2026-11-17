import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
}

// One version for the library and the demo app, set in the root gradle.properties. The version code is
// derived from it (0.1.8 -> 108, 1.2.3 -> 10203), so it rises with every release without a second number
// to keep in step.
val spotflowVersion: String = providers.gradleProperty("spotflowVersion").get()
val spotflowVersionCode: Int = spotflowVersion.substringBefore('-').split('.').map { it.toInt() }
    .also { require(it.size == 3 && it[1] < 100 && it[2] < 100) { "spotflowVersion must be MAJOR.MINOR.PATCH, not '$spotflowVersion'" } }
    .let { (major, minor, patch) -> major * 10_000 + minor * 100 + patch }

android {
    namespace = "io.spotflow.gateway.demo"
    compileSdk = 36

    defaultConfig {
        applicationId = "io.spotflow.gateway.demo"
        minSdk = 26
        targetSdk = 35
        versionCode = spotflowVersionCode
        versionName = spotflowVersion
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
        // The app shrunk by R8, the way an integrator's release build is. It exists to be built in CI:
        // that is what checks the library's consumer rules (the rules every host app inherits) still let
        // R8 through. It is not what gets released — run it on a device before that changes.
        create("minified") {
            initWith(getByName("release"))
            isMinifyEnabled = true
            isShrinkResources = true
            matchingFallbacks += "release"
            signingConfig = sharedSigning ?: signingConfigs.getByName("debug")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }


    buildFeatures {
        viewBinding = true
        buildConfig = true // BuildConfig.VERSION_NAME, shown in the header
    }

    testOptions {
        unitTests.isIncludeAndroidResources = true // required by Robolectric
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

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

dependencies {
    implementation(project(":spotflow-ble-gateway"))
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.appcompat)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.material)

    testImplementation(libs.junit)
    testImplementation(libs.robolectric)
    testImplementation(libs.androidx.test.core)
}
