package com.openlattice.organizations.tasks

import com.openlattice.authorization.SecurableObjectResolveTypeService
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.tasks.HazelcastTaskDependencies

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class OrganizationMembersCleanupDependencies(
        val securablePrincipalManager: SecurePrincipalsManager,
        val organizationService: HazelcastOrganizationService,
        val securableObjectTypes: SecurableObjectResolveTypeService
) : HazelcastTaskDependencies