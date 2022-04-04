pluginManagement {
    plugins {
        kotlin("jvm")                               version "1.6.10" apply false

        id("org.jetbrains.dokka")                       version "0.9.18" apply false
        id("com.github.spotbugs")                       version "5.0.4" apply false
        id("org.owasp.dependencycheck")                 version "6.0.1" apply false
        id("org.hidetake.swagger.generator")            version "2.18.2" apply false
        id("com.github.johnrengelman.shadow")           version "2.0.0" apply false
        id("org.jetbrains.kotlin.plugin.spring")        version "1.6.10" apply false
        id("com.github.jk1.dependency-license-report")  version "1.16" apply false
    }
    repositories {
        maven(url = "https://artifactory.openlattice.com/artifactory/gradle-release/")
        maven(url = "https://artifactory.openlattice.com/artifactory/gradle-plugins/")
        maven(url = "https://plugins.gradle.org/m2/")
        mavenCentral()
    }
}

rootProject.name="openlattice"

include("api")
include("conductor-client")
include("conductor")
include("courier")
include("datastore")
include("indexer")
include("linker")
include("launchpad")
include("mechanic")
include("neuron")
include("rhizome")
include("rhizome-client")
include("rehearsal")
include("scribe")
include("shuttle")
include("socrates")
