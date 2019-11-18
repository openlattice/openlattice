package com.openlattice.organizations.tasks

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.tasks.HazelcastTaskDependencies

data class ExternalDatabasePermissionsSyncTaskDependencies(
        val externalDBManager: ExternalDatabaseManagementService,
        val hazelcastInstance: HazelcastInstance
): HazelcastTaskDependencies