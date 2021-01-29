package com.openlattice.collaborations

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.Assembler
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory
import java.util.*

class PostgresCollaborationDatabaseService(
        hazelcast: HazelcastInstance,
        val assembler: Assembler,
        val acm: AssemblerConnectionManager,
        val externalDbConnMan: ExternalDatabaseConnectionManager
) : CollaborationDatabaseManager {

    private val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresCollaborationDatabaseService::class.java)
    }

    override fun getDatabaseInfo(collaborationId: UUID): OrganizationDatabase {
        return organizationDatabases[collaborationId]!!
    }

    override fun createCollaborationDatabase(collaborationId: UUID) {
        val dbName = ExternalDatabaseConnectionManager.buildDefaultCollaborationDatabaseName(collaborationId)
        val oid = acm.createAndInitializeCollaborationDatabase(collaborationId, dbName)

        organizationDatabases[collaborationId] = OrganizationDatabase(oid, dbName)
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