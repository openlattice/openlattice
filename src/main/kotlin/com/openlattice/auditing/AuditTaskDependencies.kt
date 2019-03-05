package com.openlattice.auditing

import com.openlattice.authorization.AuthorizationManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.tasks.HazelcastTaskDependencies

data class AuditTaskDependencies(
        val spm: SecurePrincipalsManager,
        val edmService: EdmManager,
        val authorizationManager: AuthorizationManager
) : HazelcastTaskDependencies