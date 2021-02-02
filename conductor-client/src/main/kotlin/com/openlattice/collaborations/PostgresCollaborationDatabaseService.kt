package com.openlattice.collaborations

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.Assembler
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import org.slf4j.LoggerFactory
import java.util.*

class PostgresCollaborationDatabaseService(
        hazelcast: HazelcastInstance,
        val assembler: Assembler,
        val acm: AssemblerConnectionManager,
        val externalDbConnMan: ExternalDatabaseConnectionManager,
        val authorizations: AuthorizationManager,
        val spm: SecurePrincipalsManager,
        val dbCreds: DbCredentialService
) : CollaborationDatabaseManager {

    private val collaborations = HazelcastMap.COLLABORATIONS.getMap(hazelcast)
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
        /* Identify members of added orgs to grant connect on the db to */
        val orgMemberRoles = dbCreds.getDbAccounts(
                spm.getOrganizationMembers(organizationIds).flatMap { it.value }.map { it.aclKey }.toSet()
        ).map { it.value.username }

        /* Identify schemas to create, and which pg roles should have permissions on those schemas */
        val schemaNames = organizationDatabases.getAll(organizationIds).mapValues { it.value.name }

        val orgIdToPrincipals = authorizations.getAllSecurableObjectPermissions(organizationIds.map { AclKey(it) }.toSet()).associate { acl ->
            acl.aclKey[0] to acl.aces.filter { it.permissions.contains(Permission.READ) }.map { it.principal }
        }

        val principalToAclKey = spm.getSecurablePrincipals(orgIdToPrincipals.flatMap { it.value }).associate {
            it.principal to it.aclKey
        }
        val aclKeyToDbUsername = dbCreds.getDbAccounts(principalToAclKey.values.toSet()).mapValues { it.value.username }

        val schemaNameToPostgresRoles = orgIdToPrincipals.entries.associate { (orgId, principals) ->
            schemaNames.getValue(orgId) to principals.mapNotNull { aclKeyToDbUsername[principalToAclKey[it]] }
        }

        /* Perform updates on the database */
        acm.addMembersToCollaboration(collaborationId, orgMemberRoles)
        acm.createAndInitializeSchemas(collaborationId, schemaNameToPostgresRoles)
    }

    override fun removeOrganizationsFromCollaboration(collaborationId: UUID, organizationIds: Set<UUID>) {
        /* Identify org members that should be removed */
        val allOrgIds = collaborations.getValue(collaborationId).organizationIds + organizationIds

        val allMembersByOrg = spm.getOrganizationMembers(allOrgIds)
        val remainingMembers = allMembersByOrg
                .filter { (orgId, _) -> !organizationIds.contains(orgId) }
                .flatMap { (_, members) -> members }
                .map { it.aclKey }
                .toSet()
        val membersOfRemovedOrgs = allMembersByOrg
                .filter { (orgId, _) -> organizationIds.contains(orgId) }
                .flatMap { (_, members) -> members }
                .map { it.aclKey }
                .toSet()

        val rolesToRemove = dbCreds.getDbAccounts(membersOfRemovedOrgs - remainingMembers).map { it.value.username }


        /* Identify schemas to drop */
        val schemaNames = organizationDatabases.getAll(organizationIds).map { it.value.name }

        /* Perform updates on the database */
        acm.removeMembersFromCollaboration(collaborationId, rolesToRemove)
        acm.dropSchemas(collaborationId, schemaNames)
    }

    override fun handleOrganizationDatabaseRename(collaborationId: UUID, oldName: String, newName: String) {
        acm.renameSchema(collaborationId, oldName, newName)
    }

    override fun addMembersToOrganizationInCollaborations(collaborationIds: Set<UUID>, organizationId: UUID, members: Set<AclKey>) {
        val schemaName = organizationDatabases.getValue(organizationId).name
        val usernames = dbCreds.getDbAccounts(members).values.map { it.username }
        collaborationIds.forEach {
            acm.addMembersToCollabInSchema(it, schemaName, usernames)
        }
    }

    override fun removeMembersFromOrganizationInCollaborations(collaborationIds: Set<UUID>, organizationId: UUID, members: Set<AclKey>) {
        val schemaName = organizationDatabases.getValue(organizationId).name
        val usernames = dbCreds.getDbAccounts(members).values.map { it.username }
        collaborationIds.forEach {
            acm.removeMembersFromSchemaInCollab(it, schemaName, usernames)
        }
    }
}