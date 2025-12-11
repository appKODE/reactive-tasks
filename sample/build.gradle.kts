plugins {
    alias(libs.plugins.kotlinJvm)
    application
}

application {
    mainClass.set("ru.kode.reactivetasks.MainKt")
}

dependencies {
    implementation(project(":library"))
    implementation(kotlin("stdlib"))
}
