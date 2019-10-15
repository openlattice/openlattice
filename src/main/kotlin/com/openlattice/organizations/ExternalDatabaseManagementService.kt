package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicates
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.hazelcast.HazelcastMap
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
import org.springframework.stereotype.Service
import java.sql.PreparedStatement
import java.util.*

@Service
class ExternalDatabaseManagementService(
        private val hazelcastInstance: HazelcastInstance,
        private val assemblerConfiguration: AssemblerConfiguration, //for now using this, may need to make a separate one
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager
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
                    val grantStmt = getGrantPrivilegesStmt()
                    executePrivilegesUpdate(grantStmt, dbName, it.value)
                }
                Action.REMOVE -> {
                    val revokeStmt = getRevokePrivilegesStmt()
                    executePrivilegesUpdate(revokeStmt, dbName, it.value)
                }

                Action.SET -> {
                    revokeAllPrivileges(dbName, it.value)
                    val grantStmt = getGrantPrivilegesStmt()
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

        //TODO figure out how to differentiate between creating a securable object of a table that already exists versus a new table that needs to be created
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

    fun updateOrganizationExternalDatabaseTable(orgId: UUID, tableName: String, tableId: UUID, update: MetadataUpdate) {
        if (update.name.isPresent) {
            val newTableName = update.name.get()
            val newTableFqn = FullQualifiedName(orgId.toString(), newTableName)
            aclKeyReservations.renameReservation(tableId, newTableFqn.fullQualifiedNameAsString)

            //update table name in external database
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            connect(dbName).use {
                val stmt = it.connection.prepareStatement(getRenameTableSql())
                stmt.setString(1, tableName)
                stmt.setString(2, newTableName)
                stmt.execute()
            }
        }

        organizationExternalDatabaseTables.submitToKey(tableId, UpdateOrganizationExternalDatabaseTable(update))

        //write a signalUpdate method
    }

    fun updateOrganizationExternalDatabaseColumn(orgId: UUID, tableName: String, tableId: UUID,
                                                 columnName: String, columnId: UUID, update: MetadataUpdate) {
        if (update.name.isPresent) {
            val newColumnName = update.name.get()
            val newColumnFqn = FullQualifiedName(tableId.toString(), update.name.get())
            aclKeyReservations.renameReservation(columnId, newColumnFqn.fullQualifiedNameAsString)

            //update column name in external database
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            connect(dbName).use {
                val stmt = it.connection.prepareStatement(getRenameColumnSql())
                stmt.setString(1, tableName)
                stmt.setString(2, columnName)
                stmt.setString(3, newColumnName)
                stmt.execute()
            }
        }

        organizationExternalDatabaseColumns.submitToKey(columnId, UpdateOrganizationExternalDatabaseColumn(update))

        //write a signalUpdate method
    }

    fun deleteOrganizationExternalDatabaseTables(orgId: UUID, tableNameById: Map<UUID, String>) {
        tableNameById.forEach{ deleteOrganizationExternalDatabaseTable(orgId, it.value, it.key)}
    }

    fun deleteOrganizationExternalDatabaseTable(orgId: UUID, tableName: String, tableId: UUID) {
        organizationExternalDatabaseTables.remove(tableId) //TODO make this a set so we can batch delete, entry processor?
        aclKeyReservations.release(tableId)

        //drop table from external database
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        connect(dbName).use {
            val stmt = it.connection.prepareStatement(getDropTableStmt())
            stmt.setString(1, tableName)
            stmt.execute()
        }

        //delete columns that belonged to this table
        val belongsToDeletedTable = Predicates.equal("tableId", tableId)
        val columnsToDelete = organizationExternalDatabaseColumns.values(belongsToDeletedTable)
        columnsToDelete.forEach {
            deleteOrganizationExternalDatabaseColumn(orgId, tableName, it.name, it.id)
        }

    }

    fun deleteOrganizationExternalDatabaseColumns(orgId: UUID, tableName: String, columnNameById: Map<UUID, String>) {
        columnNameById.forEach{ deleteOrganizationExternalDatabaseColumn(orgId, tableName, it.value, it.key)}
    }

    fun deleteOrganizationExternalDatabaseColumn(orgId: UUID, tableName: String, columnName: String, columnId: UUID) {
        organizationExternalDatabaseTables.remove(columnId) //TODO make this a set so we can batch delete, entry processor?
        aclKeyReservations.release(columnId)

        //drop column from table in external database
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        connect(dbName).use {
            val stmt = it.connection.prepareStatement(getDropColumnStmt())
            stmt.setString(1, tableName)
            stmt.setString(2, columnName)
            stmt.execute()
        }
    }



    /**
     * Deletes an organization's entire database.
     * Is called when an organization is deleted.
     */
    fun deleteOrganizationExternalDatabase(orgId: UUID) {
        //remove all tables/columns within org
        val belongsToDeletedDB = Predicates.equal("organizationId", orgId)
        val tablesToDelete = organizationExternalDatabaseTables.values(belongsToDeletedDB)
        tablesToDelete.forEach {
            deleteOrganizationExternalDatabaseTable(orgId, it.name, it.id)
        }

        //drop db from schema
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        connect(dbName).use {
            val stmt = it.connection.prepareStatement(getDropDBStmt())
            stmt.setString(1, dbName)
            stmt.execute()
        }
    }

    /**
     * Revokes all privileges for a user on an organization's database
     * when that user is removed from an organization.
     */
    fun revokeAllPrivilegesFromMember( orgId: UUID, userId: String ) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val userName = getDBUser(userId)
        connect(dbName).use{
            val stmt = it.connection.prepareStatement(getRevokeOnDBStmt())
            stmt.setString(1, dbName)
            stmt.setString(2, userName)
            stmt.execute()
        }
    }

    /**
     * Grants or revokes privileges on a table or column in an external database.
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
                            privileges.addAll(listOf("INSERT", "UPDATE"))
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
            val stmt = hds.connection.prepareStatement(getRevokePrivilegesStmt())
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

    private fun getGrantPrivilegesStmt(): String {
        return "GRANT ? ? ON ? TO ?"
    }

    private fun getRevokePrivilegesStmt(): String {
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

    private fun getRenameTableSql(): String {
        return "ALTER TABLE ? RENAME TO ?"
    }

    private fun getRenameColumnSql(): String {
        return "ALTER TABLE ? RENAME COLUMN ? to ?"
    }

    private fun getDropDBStmt(): String {
        return "DROP DATABASE ?"
    }

    private fun getDropTableStmt(): String {
        return "DROP TABLE IF EXISTS ?"
    }

    private fun getDropColumnStmt(): String {
        return "ALTER TABLE ? DROP COLUMN IF EXISTS ?"
    }

    private fun getRevokeOnDBStmt(): String {
        return "REVOKE ALL ON DATABASE ? FROM ?"
    }

}