package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.UUIDKeyToUUIDSetMerger
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.processors.UpdateOrganizationExternalDatabaseColumn
import com.openlattice.organizations.processors.UpdateOrganizationExternalDatabaseTable
import com.openlattice.organizations.roles.SecurePrincipalsManager

import com.openlattice.postgres.DataTables.quote
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.util.*

class ExternalDatabaseManagementService(
        private val hazelcastInstance: HazelcastInstance,
        private val assemblerConfiguration: AssemblerConfiguration, //for now using this, may need to make a separate one
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val securableObjectTypesService: SecurableObjectResolveTypeService
) {

    private val organizationExternalDatabaseColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COlUMN.name)
    private val organizationExternalDatabaseTables: IMap<UUID, OrganizationExternalDatabaseTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.name)
    private val securableObjectTypes: IMap<AclKey, SecurableObjectType> = hazelcastInstance.getMap(HazelcastMap.SECURABLE_OBJECT_TYPES.name)
    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)

    //lifted from assembly connection manager, likely will need to be customized
    companion object {
        @JvmStatic
        fun connect(dbName: String, config: Properties, useSsl: Boolean): HikariDataSource {
            config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
                "${(jdbcUrl as String).removeSuffix(
                        "/"
                )}/$dbName" + if (useSsl) {
                    "?sslmode=require"
                } else {
                    ""
                }
            }
            return HikariDataSource(HikariConfig(config))
        }
    }

    fun connect(dbName: String): HikariDataSource {
        return connect(dbName, assemblerConfiguration.server.clone() as Properties, assemblerConfiguration.ssl)
    }

    fun updatePermissionsOnAtlas(dbName: String, ipAddress: String, req: List<AclData>) {
        val permissions = req.groupBy { it.action }
        permissions.entries.forEach {
            when (it.key) {
                Action.ADD -> {
                    val grantStmt = grantPrivilegesSql()
                    executePrivilegesUpdate(grantStmt, dbName, it.value)
                }
                Action.REMOVE -> {
                    val revokeStmt = revokePrivilegesSql()
                    executePrivilegesUpdate(revokeStmt, dbName, it.value)
                }

                Action.SET -> {
                    revokeAllPrivileges(dbName, it.value)
                    val grantStmt = grantPrivilegesSql()
                    executePrivilegesUpdate(grantStmt, dbName, it.value)
                }
                else -> {
                    logger.error("Invalid action ${it.key} specified for request")
                    throw BadRequestException("Invalid action ${it.key} specified for request")
                }
            }
        }
    }

    private fun getTableAndColumnNames(aclData: AclData): Pair<String, String> {
        val aclKey = aclData.acl.aclKey
        val securableObjectType = securableObjectTypes[aclKey]!!
        val tableName: String
        val columnName: String
        val securableObjectId = aclKey.last()
        if (securableObjectType == SecurableObjectType.OrganizationAtlasColumn) {
            val organizationAtlasColumn = organizationExternalDatabaseColumns[securableObjectId]!!
            tableName = organizationExternalDatabaseTables[organizationAtlasColumn.tableId]!!.name
            columnName = organizationAtlasColumn.name
        } else {
            val organizationAtlasTable = organizationExternalDatabaseTables[securableObjectId]!!
            tableName = organizationAtlasTable.name
            columnName = ""
        }
        //add checks in here for map indexing
        return Pair(tableName, columnName)
    }

    /**
     * Creates a securable object that represents a table containing raw data in an external database
     */

    fun createOrganizationExternalDatabaseTable(orgId: UUID, table: OrganizationExternalDatabaseTable): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        val tableFQN = FullQualifiedName(orgId.toString(), table.name)
        aclKeyReservations.reserveIdAndValidateType(table, tableFQN::getFullQualifiedNameAsString)
        checkState(organizationExternalDatabaseTables.putIfAbsent(table.id, table) == null,
                "OrganizationExternalDatabaseTable ${tableFQN.fullQualifiedNameAsString} already exists")

        val tableAclKey = AclKey(orgId, table.id)
        authorizationManager.setSecurableObjectType(tableAclKey, SecurableObjectType.OrganizationAtlasTable)
        authorizationManager.addPermission(tableAclKey, principal, EnumSet.allOf(Permission::class.java))
        //eventBus?

        return table.id
    }

    /**
     * Creates a securable object that represents a column containing raw data in an external database
     */

    fun createOrganizationExternalDatabaseColumn(orgId: UUID, column: OrganizationExternalDatabaseColumn): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        checkState(organizationExternalDatabaseTables[column.tableId] == null,
                "OrganizationExternalDatabaseColumn ${column.name} belongs to a table that does not exist")
        val columnFQN = FullQualifiedName(column.tableId.toString(), column.name)
        aclKeyReservations.reserveIdAndValidateType(column, columnFQN::getFullQualifiedNameAsString)
        checkState(organizationExternalDatabaseColumns.putIfAbsent(column.id, column) == null,
                "OrganizationExternalDatabaseColumn ${columnFQN.fullQualifiedNameAsString} already exists")

        val columnAclKey = AclKey(orgId, column.tableId, column.id)
        authorizationManager.setSecurableObjectType(columnAclKey, SecurableObjectType.OrganizationAtlasColumn)
        authorizationManager.addPermission(columnAclKey, principal, EnumSet.allOf(Permission::class.java))

        return column.id
    }

    fun getOrganizationExternalDatabaseTable(tableId: UUID): OrganizationExternalDatabaseTable {
        return organizationExternalDatabaseTables[tableId]!!
    }

    fun getOrganizationExternalDatabaseColumn(columnId: UUID): OrganizationExternalDatabaseColumn {
        return organizationExternalDatabaseColumns[columnId]!!
    }

    fun updateOrganizationExternalDatabaseTable(orgId: UUID, tableId: UUID, update: MetadataUpdate) {
        if (update.name.isPresent) {
            val newTableFqn = FullQualifiedName(orgId.toString(), update.name.get())
            aclKeyReservations.renameReservation(tableId, newTableFqn.fullQualifiedNameAsString)
        }

        organizationExternalDatabaseTables.submitToKey(tableId, UpdateOrganizationExternalDatabaseTable(update))

        //write a signalUpdate method
    }

    fun updateOrganizationExternalDatabaseColumn(tableId: UUID, columnId: UUID, update: MetadataUpdate) {
        if (update.name.isPresent) {
            val newColumnFqn = FullQualifiedName(tableId.toString(), update.name.get())
            aclKeyReservations.renameReservation(columnId, newColumnFqn.fullQualifiedNameAsString)
        }

        organizationExternalDatabaseColumns.submitToKey(columnId, UpdateOrganizationExternalDatabaseColumn(update))

        //write a signalUpdate method
    }

    fun deleteOrganizationExternalDatabaseTable(tableId: UUID) {
        organizationExternalDatabaseTables.remove(tableId)
        aclKeyReservations.release(tableId)

        //delete columns that belonged to this table
        val belongsToDeletedTable = Predicates.equal("tableId", tableId)
        val columnsToDelete = organizationExternalDatabaseColumns.values(belongsToDeletedTable)
        columnsToDelete.forEach {
            deleteOrganizationExternalDatabaseColumn(it.id)
        }

    }

    fun deleteOrganizationExternalDatabaseColumn(columnId: UUID) {
        organizationExternalDatabaseTables.remove(columnId)
        aclKeyReservations.release(columnId)
    }

    /**
     * Grants or revokes privileges on a table or column in an external database
     */
    private fun executePrivilegesUpdate(sqlStmt: String, dbName: String, aclData: List<AclData>) {
        connect(dbName).use { hds ->
            val stmt = hds.connection.prepareStatement(sqlStmt)
            aclData.forEach {
                val tableAndColumnNames = getTableAndColumnNames(it)
                it.acl.aces.forEach { ace ->
                    val dbUser = getDBUser(ace.principal.id)
                    val privileges = mutableListOf<String>()
                    if (ace.permissions.contains(Permission.OWNER)) {
                        privileges.add("ALL")
                    } else {
                        if (ace.permissions.contains(Permission.WRITE)) {
                            val writePrivileges = listOf("INSERT", "UPDATE", "DELETE")
                            privileges.addAll(writePrivileges)
                        }
                        if (ace.permissions.contains(Permission.READ)) {
                            privileges.add("SELECT")
                        }
                    }
                    preparePrivilegesStmt(stmt, privileges, tableAndColumnNames.first, tableAndColumnNames.second, dbUser)
                }
            }
            stmt.executeBatch()
        }
    }


    private fun revokeAllPrivileges(dbName: String, aclData: List<AclData>) {
        connect(dbName).use { hds ->
            val stmt = hds.connection.prepareStatement(revokePrivilegesSql())
            aclData.forEach {
                val tableAndColumnNames = getTableAndColumnNames(it)
                it.acl.aces.forEach { ace ->
                    val dbUser = getDBUser(ace.principal.id)
                    preparePrivilegesStmt(stmt, listOf("ALL"), tableAndColumnNames.first, tableAndColumnNames.second, dbUser)
                }
            }
            stmt.executeBatch()

        }
    }

    private fun getDBUser(principalId: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principalId)
        return quote(buildPostgresUsername(securePrincipal))
    }

    private fun grantPrivilegesSql(): String {
        return "GRANT ? ? ON ? TO ?"
    }

    private fun revokePrivilegesSql(): String {
        return "REVOKE ? ? ON ? FROM ?"
    }

    private fun preparePrivilegesStmt(stmt: PreparedStatement, privileges: List<String>, tableName: String, columnName: String, dbUser: String) {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        stmt.setString(1, privilegesAsString)
        stmt.setString(2, tableName)
        stmt.setString(3, columnName)
        stmt.setString(4, dbUser)
        stmt.addBatch()
    }

}