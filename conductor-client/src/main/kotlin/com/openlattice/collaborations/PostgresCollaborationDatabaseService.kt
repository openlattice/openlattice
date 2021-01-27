package com.openlattice.collaborations

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.Assembler
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationDatabase
import java.util.*

class PostgresCollaborationDatabaseService(
        hazelcast: HazelcastInstance,
        val assembler: Assembler
) : CollaborationDatabaseManager {

    private val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(hazelcast)

    override fun createCollaborationDatabase(collaboration: Collaboration) {
        val databaseInfo = assembler.createCollaborationDatabaseAndReturnOid(collaboration.id)
        organizationDatabases[collaboration.id] = databaseInfo
    }

    override fun deleteCollaborationDatabase(collaborationId: UUID) {
        TODO("Not yet implemented")
    }

    override fun renameCollaborationDatabase(collaborationId: UUID, newName: String) {
        val currentName = organizationDatabases.getValue(collaborationId).name
        assembler.renameDatabase(currentName, newName)
    }

    override fun addOrganizationsToCollaboration(collaborationId: UUID, organizationIds: Set<UUID>) {
        TODO("Not yet implemented")
    }

    override fun getDatabaseInfo(collaborationId: UUID): OrganizationDatabase {
        return organizationDatabases[collaborationId]!!
    }

    override fun removeOrganizationsFromCollaboration(collaborationId: UUID, organizationIds: Set<UUID>) {
        TODO("Not yet implemented")
    }
}