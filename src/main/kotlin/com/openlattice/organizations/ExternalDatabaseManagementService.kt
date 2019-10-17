package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicates
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManager.Companion.PUBLIC_SCHEMA
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
import kotlin.collections.LinkedHashMap

@Service
class ExternalDatabaseManagementService(
        private val hazelcastInstance: HazelcastInstance,
        private val assemblerConnectionManager: AssemblerConnectionManager, //for now using this, may need to move connection logic to its own file
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager
) {

    private val organizationExternalDatabaseColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COlUMN.name)
    private val organizationExternalDatabaseTables: IMap<UUID, OrganizationExternalDatabaseTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.name)
    private val securableObjectTypes: IMap<AclKey, SecurableObjectType> = hazelcastInstance.getMap(HazelcastMap.SECURABLE_OBJECT_TYPES.name)
    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)

    fun updateExternalDatabasePermissions(dbName: String, ipAddress: String, req: List<AclData>) {
        val permissions = req.groupBy { it.action }
        permissions.entries.forEach {
            when (it.key) {
                Action.ADD -> {
                    executePrivilegesUpdate(Action.ADD, dbName, it.value)
                }
                Action.REMOVE -> {
                    executePrivilegesUpdate(Action.REMOVE, dbName, it.value)
                }

                Action.SET -> {
                    revokeAllPrivileges(dbName, it.value)
                    executePrivilegesUpdate(Action.ADD, dbName, it.value)
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

    fun createOrganizationExternalDatabaseTable(orgId: UUID, table: OrganizationExternalDatabaseTable): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        val tableFQN = FullQualifiedName(orgId.toString(), table.name)
        checkState(organizationExternalDatabaseTables.putIfAbsent(table.id, table) == null,
                "OrganizationExternalDatabaseTable ${tableFQN.fullQualifiedNameAsString} already exists")
        aclKeyReservations.reserveIdAndValidateType(table, tableFQN::getFullQualifiedNameAsString)

        val tableAclKey = AclKey(orgId, table.id)
        authorizationManager.setSecurableObjectType(tableAclKey, SecurableObjectType.OrganizationAtlasTable)
        authorizationManager.addPermission(tableAclKey, principal, EnumSet.allOf(Permission::class.java))
        //eventBus?

        return table.id
    }

    fun createOrganizationExternalDatabaseColumn(orgId: UUID, column: OrganizationExternalDatabaseColumn): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        checkState(organizationExternalDatabaseTables[column.tableId] != null,
                "OrganizationExternalDatabaseColumn ${column.name} belongs to a table that does not exist")
        val columnFQN = FullQualifiedName(column.tableId.toString(), column.name)
        checkState(organizationExternalDatabaseColumns.putIfAbsent(column.id, column) == null,
                "OrganizationExternalDatabaseColumn ${columnFQN.fullQualifiedNameAsString} already exists")
        aclKeyReservations.reserveIdAndValidateType(column, columnFQN::getFullQualifiedNameAsString)

        val columnAclKey = AclKey(orgId, column.tableId, column.id)
        authorizationManager.setSecurableObjectType(columnAclKey, SecurableObjectType.OrganizationAtlasColumn)
        authorizationManager.addPermission(columnAclKey, principal, EnumSet.allOf(Permission::class.java))

        return column.id
    }

    fun createNewOrganizationExternalDatabaseTable(orgId: UUID, tableName: String, columnNameToSqlType: LinkedHashMap<String, String>) {
        //TODO figure out title stuff
        val table = OrganizationExternalDatabaseTable(Optional.empty(), tableName, "title", Optional.empty(), orgId)
        val tableId = createOrganizationExternalDatabaseTable(orgId, table)
        columnNameToSqlType.keys.forEach {
            val column = OrganizationExternalDatabaseColumn(Optional.empty(), it, "title", Optional.empty(), tableId, orgId)
            createOrganizationExternalDatabaseColumn(orgId, column)
        }

        //create table in db
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        assemblerConnectionManager.connect(dbName).use {
            it.connection.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE $PUBLIC_SCHEMA.$tableName(${columnNameToSqlType
                        .map { entry -> entry.key + " " + entry.value }.joinToString(", ")})")
            }

        }
    }

    fun createNewOrganizationExternalDatabaseColumn(orgId: UUID, tableId: UUID, tableName: String, columnName: String, sqlType: String) {
        //TODO figure out title stuff
        val column = OrganizationExternalDatabaseColumn(Optional.empty(), columnName, "title", Optional.empty(), tableId, orgId)
        createOrganizationExternalDatabaseColumn(orgId, column)

        //add column to table in db
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        assemblerConnectionManager.connect(dbName).use {
            it.connection.createStatement().use { stmt ->
                stmt.execute("ALTER TABLE $PUBLIC_SCHEMA.$tableName ADD COLUMN $columnName $sqlType")
            }

        }
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
            val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
            val newTableNamePath = "$PUBLIC_SCHEMA.$newTableName"
            assemblerConnectionManager.connect(dbName).use {
                it.connection.createStatement().use { stmt ->
                    stmt.execute("ALTER TABLE $tableNamePath RENAME TO $newTableName")
                }
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
            val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
            assemblerConnectionManager.connect(dbName).use {
                it.connection.createStatement().use { stmt ->
                    stmt.execute("ALTER TABLE $tableNamePath RENAME COLUMN $columnName to $newColumnName")
                }
            }
        }

        organizationExternalDatabaseColumns.submitToKey(columnId, UpdateOrganizationExternalDatabaseColumn(update))

        //write a signalUpdate method
    }

    fun deleteOrganizationExternalDatabaseTables(orgId: UUID, tableNameById: Map<UUID, String>) {
        tableNameById.forEach { deleteOrganizationExternalDatabaseTable(orgId, it.value, it.key) }
    }

    fun deleteOrganizationExternalDatabaseTable(orgId: UUID, tableName: String, tableId: UUID) {
        organizationExternalDatabaseTables.remove(tableId) //TODO make this a set so we can batch delete, entry processor?
        aclKeyReservations.release(tableId)

        //drop table from external database
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
        assemblerConnectionManager.connect(dbName).use {
            it.connection.createStatement().use { stmt ->
                stmt.execute("DROP TABLE IF EXISTS $tableNamePath")
            }
        }

        //delete columns that belonged to this table
        val belongsToDeletedTable = Predicates.equal("tableId", tableId)
        val columnsToDelete = organizationExternalDatabaseColumns.values(belongsToDeletedTable)
        columnsToDelete.forEach {
            deleteOrganizationExternalDatabaseColumn(orgId, tableName, it.name, it.id)
        }

    }

    fun deleteOrganizationExternalDatabaseColumns(orgId: UUID, tableName: String, columnNameById: Map<UUID, String>) {
        columnNameById.forEach { deleteOrganizationExternalDatabaseColumn(orgId, tableName, it.value, it.key) }
    }

    fun deleteOrganizationExternalDatabaseColumn(orgId: UUID, tableName: String, columnName: String, columnId: UUID) {
        organizationExternalDatabaseTables.remove(columnId) //TODO make this a set so we can batch delete, entry processor?
        aclKeyReservations.release(columnId)

        //drop column from table in external database
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
        assemblerConnectionManager.connect(dbName).use {
            it.connection.createStatement().use { stmt ->
                stmt.execute("ALTER TABLE $tableNamePath DROP COLUMN IF EXISTS $columnName")
            }
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
        assemblerConnectionManager.connect(dbName).use {
            it.connection.createStatement().use { stmt ->
                stmt.execute("DROP DATABASE $dbName")
            }
        }
    }

    /**
     * Revokes all privileges for a user on an organization's database
     * when that user is removed from an organization.
     */
    fun revokeAllPrivilegesFromMember(orgId: UUID, userId: String) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val userName = getDBUser(userId)
        assemblerConnectionManager.connect(dbName).use {
            it.connection.createStatement().use { stmt ->
                stmt.execute("REVOKE ALL ON DATABASE $dbName FROM $userName")
            }
        }
    }

    /**
     * Grants or revokes privileges on a table or column in an external database.
     */
    private fun executePrivilegesUpdate(action: Action, dbName: String, aclData: List<AclData>) {
        assemblerConnectionManager.connect(dbName).use { hds ->
            hds.connection.createStatement().use { stmt ->
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
                        val sql = createPrivilegesSql(action, privileges, tableAndColumnNames.first, tableAndColumnNames.second, dbUser)
                        stmt.addBatch(sql)
                    }
                }
                stmt.executeBatch()
            }
        }
    }


    private fun revokeAllPrivileges(dbName: String, aclData: List<AclData>) {
        assemblerConnectionManager.connect(dbName).use { hds ->
            hds.connection.createStatement().use { stmt ->
                aclData.forEach {
                    val tableAndColumnNames = getTableAndColumnNames(it)
                    it.acl.aces.forEach { ace ->
                        val dbUser = getDBUser(ace.principal.id)
                        val sql = createPrivilegesSql(Action.REMOVE, listOf("ALL"), tableAndColumnNames.first, tableAndColumnNames.second, dbUser)
                        stmt.addBatch(sql)
                    }
                }
                stmt.executeBatch()
            }
        }
    }

    private fun getDBUser(principalId: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principalId)
        return quote(buildPostgresUsername(securePrincipal))
    }

    private fun preparePrivilegesStmt(stmt: PreparedStatement, privileges: List<String>, tableName: String, columnName: String, dbUser: String) {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
        stmt.setString(1, privilegesAsString)
        stmt.setString(2, tableNamePath)
        stmt.setString(3, columnName)
        stmt.setString(4, dbUser)
        stmt.addBatch()
    }

    private fun createPrivilegesSql(action: Action, privileges: List<String>, tableName: String, columnName: String, dbUser: String): String {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
        return if (action == Action.ADD) {
            "GRANT $privilegesAsString $columnName ON $tableNamePath TO $dbUser"
        } else {
            "REVOKE $privilegesAsString $columnName ON $tableNamePath FROM $dbUser"
        }
    }

}