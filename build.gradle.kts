description = "OpenLattice Development Environment"

tasks.create("getVersions") {
    doLast {
        logger.warn("\nCurrent project versions: ")
        subprojects.forEach { project ->
            logger.warn("${project.name}:${project.version}")
        }
    }
}
