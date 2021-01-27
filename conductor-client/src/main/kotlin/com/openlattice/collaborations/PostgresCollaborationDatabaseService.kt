package com.openlattice.collaborations

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.Assembler
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationDatabase
import java.util.*

class PostgresCollaborationDatabaseService(
        hazelcast: HazelcastInstance,
        val assembler: Assembler,
        val acm: AssemblerConnectionManager
) : CollaborationDatabaseManager {

    private val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(hazelcast)

    override fun getDatabaseInfo(collaborationId: UUID): OrganizationDatabase {
        return organizationDatabases[collaborationId]!!
    }

    override fun createCollaborationDatabase(collaborationId: UUID) {
        val databaseInfo = assembler.createCollaborationDatabaseAndReturnOid(collaborationId)
        organizationDatabases[collaborationId] = databaseInfo
    }

    override fun deleteCollaborationDatabase(collaborationId: UUID) {
        acm.dropDatabase(getDatabaseInfo(collaborationId).name)
    }

    override fun renameCollaborationDatabase(collaborationId: UUID, newName: String) {
        val currentName = organizationDatabases.getValue(collaborationId).name
        assembler.renameDatabase(currentName, newName)
    }

    override fun addOrganizationsToCollaboration(collaborationId: UUID, organizationIds: Set<UUID>) {
        TODO("Not yet implemented")
    }

    override fun removeOrganizationsFromCollaboration(collaborationId: UUID, organizationIds: Set<UUID>) {
        TODO("Not yet implemented")
    }
}