package com.openlattice.organizations.tasks

import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.organization.OrganizationPrincipal
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(OrganizationMembersCleanupInitializationTask::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationMembersCleanupInitializationTask : HazelcastInitializationTask<OrganizationMembersCleanupDependencies> {
    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: OrganizationMembersCleanupDependencies) {
        dependencies
                .securableObjectTypes.getSecurableObjectsOfType(SecurableObjectType.Organization)
                .mapNotNull(dependencies.securablePrincipalManager::getSecurablePrincipal)
                .filter { principal -> principal !is OrganizationPrincipal }
                .map { principal -> dependencies.organizationService.getOrganization(principal.id) }
                .forEach { org ->
                    val forRemoval = org.members
                            .filterNot(dependencies.securablePrincipalManager::principalExists)
                            .toSet()
                    logger.error("Removing non-existent principals {} from organization {}", forRemoval, org.id)
                    dependencies.organizationService.removeMembers(org.id, forRemoval)
                }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(OrganizationsInitializationTask::class.java)
    }

    override fun getName(): String {
        return Task.ORGANIZATION_MEMBERS_CLEANUP.name
    }

    override fun getDependenciesClass(): Class<out OrganizationMembersCleanupDependencies> {
        return OrganizationMembersCleanupDependencies::class.java
    }
}
