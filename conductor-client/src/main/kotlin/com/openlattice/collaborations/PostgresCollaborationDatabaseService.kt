package com.openlattice.collaborations

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Permission
import com.openlattice.collaborations.mapstores.ProjectedTablesMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationDatabase
import com.openlattice.organizations.mapstores.TABLE_ID_INDEX
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.PostgresProjectionService
import com.openlattice.postgres.external.DatabaseQueryManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.external.Schemas
import org.slf4j.LoggerFactory
import java.util.*

class PostgresCollaborationDatabaseService(
        hazelcast: HazelcastInstance,
        private val dbQueryManager: DatabaseQueryManager,
        private val externalDbConnMan: ExternalDatabaseConnectionManager,
        private val authorizations: AuthorizationManager,
        private val externalDbPermissioner: ExternalDatabasePermissioningService,
        private val spm: SecurePrincipalsManager,
        private val dbCreds: DbCredentialService,
        private val assemblerConfiguration: AssemblerConfiguration
) : CollaborationDatabaseManager {

    private val collaborations = HazelcastMap.COLLABORATIONS.getMap(hazelcast)
    private val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(hazelcast)
    private val externalTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcast)
    private val externalColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(hazelcast)
    private val projectedTables = HazelcastMap.PROJECTED_TABLES.getMap(hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresCollaborationDatabaseService::class.java)
    }

    override fun getDatabaseInfo(collaborationId: UUID): OrganizationDatabase {
        return organizationDatabases[collaborationId]!!
    }

    override fun createCollaborationDatabase(collaborationId: UUID) {
        val dbName = ExternalDatabaseConnectionManager.buildDefaultCollaborationDatabaseName(collaborationId)
        val oid = dbQueryManager.createAndInitializeCollaborationDatabase(collaborationId, dbName)

        organizationDatabases[collaborationId] = OrganizationDatabase(oid, dbName)
    }

    override fun deleteCollaborationDatabase(collaborationId: UUID) {
        dbQueryManager.dropDatabase(getDatabaseInfo(collaborationId).name)
    }

    override fun renameCollaborationDatabase(collaborationId: UUID, newName: String) {
        val currentName = organizationDatabases.getValue(collaborationId).name
        dbQueryManager.renameDatabase(currentName, newName)
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
        dbQueryManager.addMembersToCollaboration(collaborationId, orgMemberRoles)
        dbQueryManager.createAndInitializeSchemas(collaborationId, schemaNameToPostgresRoles)
        organizationIds.forEach { createOrganizationFdw(collaborationId, it) }
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

        /* Identify table ids to remove */
        val projectedTableIds = projectedTables.entrySet(Predicates.and(
                collaborationIdPredicate(collaborationId),
                organizationIdsPredicate(organizationIds)
        )).associate { it.key.tableId to it.value.organizationId }

        /* Identify schemas to drop */
        val schemaNames = organizationDatabases.getAll(organizationIds).map { it.value.name }

        /* Perform updates on the database */
        projectedTableIds.forEach { (tableId, organizationId) ->
            removeTableProjection(collaborationId, organizationId, tableId)
            projectedTables.delete(ProjectedTableKey(tableId, collaborationId))
        }
        dbQueryManager.removeMembersFromCollaboration(collaborationId, rolesToRemove)
        dbQueryManager.dropSchemas(collaborationId, schemaNames)
    }

    override fun handleOrganizationDatabaseRename(collaborationId: UUID, organizationId: UUID, oldName: String, newName: String) {
        dbQueryManager.renameSchema(collaborationId, oldName, newName)
        PostgresProjectionService.changeDbNameForFdw(
                externalDbConnMan.connectToOrg(collaborationId),
                getFdwName(organizationId),
                newName
        )
    }

    override fun addMembersToOrganizationInCollaborations(collaborationIds: Set<UUID>, organizationId: UUID, members: Set<AclKey>) {
        val schemaName = organizationDatabases.getValue(organizationId).name
        val usernames = dbCreds.getDbAccounts(members).values.map { it.username }
        collaborationIds.forEach {
            dbQueryManager.addMembersToCollabInSchema(it, schemaName, usernames)
        }
    }

    override fun removeMembersFromOrganizationInCollaboration(
            collaborationId: UUID,
            organizationId: UUID,
            membersToRemoveFromSchema: Set<AclKey>,
            membersToRemoveFromDatabase: Set<AclKey>
    ) {
        val schemaName = organizationDatabases.getValue(organizationId).name
        val roleNamesByAclKey = dbCreds.getDbAccounts(membersToRemoveFromSchema + membersToRemoveFromDatabase).mapValues { it.value.username }

        val roleNamesToRemoveFromDatabase = membersToRemoveFromDatabase.mapNotNull { roleNamesByAclKey[it] }

        dbQueryManager.removeMembersFromSchemaInCollab(collaborationId, schemaName, roleNamesByAclKey.values)
        dbQueryManager.removeMembersFromDatabaseInCollab(collaborationId, roleNamesToRemoveFromDatabase)
    }

    override fun initializeTableProjection(collaborationId: UUID, organizationId: UUID, tableId: UUID) {
        val collaborationHds = externalDbConnMan.connectToOrg(collaborationId)
        val table = externalTables.getValue(tableId)
        val columns = externalColumns.values(Predicates.equal(TABLE_ID_INDEX, tableId)).sortedBy { it.ordinalPosition }.toSet()

        val foreignSchema = table.schema
        val foreignName = table.name

        val intermediateSchema = Schemas.PROJECTIONS_SCHEMA.label
        val intermediateName = tableId.toString()

        val viewSchema = externalDbConnMan.getOrganizationDatabaseName(organizationId)
        val viewName = table.name

        PostgresProjectionService.importTableFromFdw(
                hds = collaborationHds,
                fdwName = getFdwName(organizationId),
                sourceSchema = foreignSchema,
                sourceTableName = foreignName,
                destinationSchema = intermediateSchema,
                destinationTableName = intermediateName
        )

        PostgresProjectionService.createViewOverTable(
                hds = collaborationHds,
                sourceTableSchema = intermediateSchema,
                sourceTableName = intermediateName,
                viewSchema = viewSchema,
                viewName = viewName
        )

        externalDbPermissioner.initializeProjectedTableViewPermissions(
                collaborationId,
                viewSchema,
                table,
                columns
        )
    }

    override fun removeTableProjection(collaborationId: UUID, organizationId: UUID, tableId: UUID) {
        val table = externalTables.getValue(tableId)
        val orgSchema = externalDbConnMan.getOrganizationDatabaseName(organizationId)
        val collaborationHds = externalDbConnMan.connectToOrg(collaborationId)

        PostgresProjectionService.destroyViewOverTable(collaborationHds, orgSchema, table.name)
        PostgresProjectionService.dropTableImportedFromFdw(collaborationHds, Schemas.PROJECTIONS_SCHEMA.label, table.id.toString())
    }

    override fun refreshTableProjection(collaborationId: UUID, organizationId: UUID, tableId: UUID) {
        removeTableProjection(collaborationId, organizationId, tableId)
        initializeTableProjection(collaborationId, organizationId, tableId)
    }

    private fun createOrganizationFdw(collaborationId: UUID, organizationId: UUID) {
        val organizationDbName = externalDbConnMan.getOrganizationDatabaseName(organizationId)
        val fdwName = getFdwName(organizationId)

        val atlasUsername = assemblerConfiguration.server.getProperty("username")
        val atlasPassword = assemblerConfiguration.server.getProperty("password")

        val collaborationHds = externalDbConnMan.connectToOrg(collaborationId)
        val orgDbJdbc = externalDbConnMan.appendDatabaseToJdbcPartial(
                assemblerConfiguration.server.getProperty("jdbcUrl"),
                organizationDbName
        )

        PostgresProjectionService.createFdwBetweenDatabases(
                localDbDatasource = collaborationHds,
                remoteUser = atlasUsername,
                remotePassword = atlasPassword,
                remoteDbJdbc = orgDbJdbc,
                localUsername = atlasUsername,
                localSchema = Schemas.PROJECTIONS_SCHEMA,
                fdwName = fdwName
        )
    }

    private fun getFdwName(organizationId: UUID): String {
        return "fdw_${organizationId.toString().replace("-", "")}"
    }

    private fun collaborationIdPredicate(collaborationId: UUID): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.equal(ProjectedTablesMapstore.COLLABORATION_ID_INDEX, collaborationId)
    }

    private fun organizationIdsPredicate(organizationIds: Collection<UUID>): Predicate<ProjectedTableKey, ProjectedTableMetadata> {
        return Predicates.`in`(ProjectedTablesMapstore.ORGANIZATION_ID_INDEX, *organizationIds.toTypedArray())
    }

}