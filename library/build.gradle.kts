import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.android.kotlin.multiplatform.library)
    alias(libs.plugins.vanniktech.mavenPublish)
}

group = providers.gradleProperty("pomGroupId").get()
version = providers.gradleProperty("versionName").get()

kotlin {
    jvm()
    android {
        namespace = "ru.kode.reactivetasks"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        withHostTest {}

        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_17)
        }
    }
    iosX64()
    iosArm64()
    iosSimulatorArm64()

    sourceSets {
        commonMain.dependencies {
            api(libs.kotlin.coroutines)
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
        }

        iosMain.dependencies {
            implementation(libs.kotlinx.atomicfu)
        }
    }
}

mavenPublishing {
    coordinates(artifactId = "reactive-tasks")

    publishToMavenCentral()
    signAllPublications()

    pom {
        val pomName: String by project
        val pomDescription: String by project
        val pomUrl: String by project
        val pomScmUrl: String by project
        val pomScmConnection: String by project
        val pomScmDevConnection: String by project
        val pomLicenseName: String by project
        val pomLicenseUrl: String by project
        val pomLicenseDist: String by project
        val pomDeveloperId: String by project
        val pomDeveloperName: String by project

        name.set(pomName)
        description.set(pomDescription)
        url.set(pomUrl)

        scm {
            url.set(pomScmUrl)
            connection.set(pomScmConnection)
            developerConnection.set(pomScmDevConnection)
        }

        licenses {
            license {
                name.set(pomLicenseName)
                url.set(pomLicenseUrl)
                distribution.set(pomLicenseDist)
            }
        }

        developers {
            developer {
                id.set(pomDeveloperId)
                name.set(pomDeveloperName)
            }
        }
    }
}
