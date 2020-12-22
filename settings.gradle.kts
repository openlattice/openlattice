pluginManagement {
    plugins {
        kotlin("jvm")                                   version "1.3.72" apply false

        id("org.jetbrains.dokka")                       version "0.9.18" apply false
        id("com.github.spotbugs")                       version "4.6.0" apply false
        id("org.owasp.dependencycheck")                 version "6.0.1" apply false
        id("org.hidetake.swagger.generator")            version "2.18.1" apply false
        id("com.github.johnrengelman.shadow")           version "2.0.0" apply false
        id("org.jetbrains.kotlin.plugin.spring")        version "1.3.72" apply false
        id("com.github.jk1.dependency-license-report")  version "1.11" apply false
    }
    repositories {
        maven(url = "https://artifactory.openlattice.com/artifactory/gradle-release/")
        mavenCentral()
    }
}

rootProject.name="openlattice"

include("api")
include("chronicle-api")
include("chronicle-server")
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
include("scrunchie")
include("shuttle")
include("socrates")
