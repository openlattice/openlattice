package com.openlattice.auditing

import com.openlattice.authorization.AuthorizationManager
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.tasks.HazelcastTaskDependencies

data class AuditTaskDependencies(
        val spm: SecurePrincipalsManager,
        val entitySetManager: EntitySetManager,
        val authorizationManager: AuthorizationManager,
        val partitionManager: PartitionManager,
        val organizationsManager: HazelcastOrganizationService
) : HazelcastTaskDependencies