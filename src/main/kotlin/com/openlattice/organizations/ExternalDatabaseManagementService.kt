package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicates
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
import com.openlattice.postgres.ResultSetAdapters.organizationId
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import kotlin.collections.LinkedHashMap

@Service
class ExternalDatabaseManagementService(
        private val hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        private val assemblerConnectionManager: AssemblerConnectionManager, //for now using this, may need to move connection logic to its own file
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager
) {

    private val organizationExternalDatabaseColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COlUMN.name)
    private val organizationExternalDatabaseTables: IMap<UUID, OrganizationExternalDatabaseTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.name)
    private val securableObjectTypes: IMap<AclKey, SecurableObjectType> = hazelcastInstance.getMap(HazelcastMap.SECURABLE_OBJECT_TYPES.name)
    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)

    fun updateExternalDatabasePermissions(dbName: String, req: List<AclData>) {
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

    fun addTrustedUser(orgId: UUID, userPrincipal: Principal, ipAdresses: Set<String>) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val userName = getDBUser(userPrincipal.id)
        //edit the pg_hba file through some magic. must. become. magician.
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

    fun setPrimaryKey(orgId: UUID, tableName: String, tableId: UUID, columnNames: Set<String>) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
        assemblerConnectionManager.connect(dbName).use {
            //remove primary key if it exists
            it.connection.createStatement().use { stmt ->
                val primaryKeyResultSet = stmt.executeQuery(
                        "SELECT constraint_name FROM information_schema.table_constraints " +
                                "WHERE table_name = $tableNamePath AND constraint_type = 'PRIMARY KEY'")
                if (primaryKeyResultSet.isBeforeFirst) {
                    val primaryKey = primaryKeyResultSet.getString("constraint_name")
                    stmt.execute("ALTER TABLE $tableNamePath DROP CONSTRAINT $primaryKey")
                }
            }
            if (columnNames.isNotEmpty()) {
                val primaryKeyColumns = columnNames.joinToString(", ")
                it.connection.createStatement().use { stmt ->
                    stmt.execute("ALTER TABLE $tableNamePath ADD PRIMARY KEY ($primaryKeyColumns)")

                }
            }
        }
    }

    fun deleteOrganizationExternalDatabaseTables(orgId: UUID, tableIds: Set<UUID>) {
        tableIds.forEach { deleteOrganizationExternalDatabaseTable(orgId, it) }
    }

    fun deleteOrganizationExternalDatabaseTable(orgId: UUID, tableId: UUID) {
        organizationExternalDatabaseTables.remove(tableId) //TODO make this a set so we can batch delete, entry processor?
        aclKeyReservations.release(tableId)

        //delete columns that belonged to this table
        val belongsToDeletedTable = Predicates.equal("tableId", tableId)
        val columnsToDelete = organizationExternalDatabaseColumns.values(belongsToDeletedTable)
        val columnIds = columnsToDelete.map { it.id }.toSet()
        deleteOrganizationExternalDatabaseColumns(orgId, columnIds)

    }

    fun deleteOrganizationExternalDatabaseColumns(orgId: UUID, columnIds: Set<UUID>) {
        columnIds.forEach { deleteOrganizationExternalDatabaseColumn(orgId, it) }
    }

    fun deleteOrganizationExternalDatabaseColumn(orgId: UUID, columnId: UUID) {
        organizationExternalDatabaseColumns.remove(columnId) //TODO make this a set so we can batch delete, entry processor?
        aclKeyReservations.release(columnId)
    }

    /**
     * Deletes an organization's entire database.
     * Is called when an organization is deleted.
     */
    //TODO break into further helper methods to reduce reuse of code
    fun deleteOrganizationExternalDatabase(orgId: UUID) {
        //remove all tables/columns within org
        val belongsToDeletedDB = Predicates.equal("organizationId", orgId)
        val tablesToDelete = organizationExternalDatabaseTables.values(belongsToDeletedDB)
        tablesToDelete.forEach { table ->
            organizationExternalDatabaseTables.remove(table.id)
            aclKeyReservations.release(table.id)
            val belongsToDeletedTable = Predicates.equal("tableId", table.id)
            val columnsToDelete = organizationExternalDatabaseColumns.values(belongsToDeletedTable)
            columnsToDelete.forEach { column ->
                organizationExternalDatabaseColumns.remove(column.id)
                aclKeyReservations.release(column.id)
            }
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

    fun getOrganizationDBNames(): Set<UUID> {
        val orgIds = mutableSetOf<UUID>()
        hds.connection.createStatement().use {
            val rs = it.executeQuery("SELECT id FROM organizations")
            while (rs.next()) {
                orgIds.add(organizationId(rs))
            }
        }
        return orgIds
    }

    fun getCurrentTables(dbName: String): Set<String> {
        val tableNames = mutableSetOf<String>()
        assemblerConnectionManager.connect(dbName).use { dataSource ->
            dataSource.connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema='$PUBLIC_SCHEMA' " +
                        "AND table_type='BASE TABLE'")
                while (rs.next()) {
                    val tableName = rs.getString("table_name")
                    tableNames.add(tableName)
                }

            }
        }
        return tableNames
    }

    fun getColumnNamesByTable(orgId: UUID, dbName: String): Map<String, Set<String>> {
        val columnNamesByTableName = HashMap<String, MutableSet<String>>()
        assemblerConnectionManager.connect(dbName).use { dataSource ->
            dataSource.connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        "SELECT information_schema.tables.table_name, information_schema.columns.column_name " +
                                "FROM information_schema.tables " +
                                "LEFT JOIN information_schema.columns on " +
                                "information_schema.tables.table_name = information_schema.columns.table_name " +
                                "WHERE information_schema.tables.table_schema='$PUBLIC_SCHEMA' " +
                                "AND table_type='BASE TABLE' " +
                                "ORDER BY information_schema.tables.table_name")
                while (rs.next()) {
                    val tableName = rs.getString("table_name")
                    val columnName = rs.getString("column_name")
                    columnNamesByTableName.getOrPut(tableName) { mutableSetOf() }.add(columnName)
                }
            }

        }
        return columnNamesByTableName
    }

    fun createNewColumnObjects(dbName: String, tableName: String, tableId: UUID, orgId: UUID, columnName: Optional<String>): Set<OrganizationExternalDatabaseColumn> {
        val columnCondition = if (columnName.isPresent) {
            "AND information_schema.columns.column_name = '${columnName.get()}'"
        } else {
            ""
        }
        val newColumns = mutableSetOf<OrganizationExternalDatabaseColumn>()
        assemblerConnectionManager.connect(dbName).use { dataSource ->
            dataSource.connection.createStatement().use { stmt ->
                val rs = stmt.executeQuery(
                        "SELECT information_schema.tables.table_name, information_schema.columns.column_name, " +
                                "information_schema.columns.data_type, information_schema.columns.ordinal_position, " +
                                "information_schema.table_constraints.constraint_type " +
                                "FROM information_schema.tables " +
                                "LEFT JOIN information_schema.columns on information_schema.tables.table_name = " +
                                "information_schema.columns.table_name " +
                                "LEFT OUTER JOIN information_schema.constraint_column_usage on " +
                                "information_schema.columns.column_name = information_schema.constraint_column_usage.column_name " +
                                "LEFT OUTER JOIN information_schema.table_constraints " +
                                "on information_schema.constraint_column_usage.constraint_name = " +
                                "information_schema.table_constraints.constraint_name " +
                                "WHERE information_schema.columns.table_name = '$tableName' " +
                                "$columnCondition"
                )
                while (rs.next()) {
                    val columnName = rs.getString("column_name")
                    val dataType = rs.getString("data_type")
                    val position = rs.getInt( "ordinal_position")
                    val isPrimaryKey = rs.getString("constraint_type") == "PRIMARY KEY"
                    val newColumn = OrganizationExternalDatabaseColumn(
                            Optional.empty(),
                            columnName,
                            columnName,
                            Optional.empty(),
                            tableId,
                            orgId,
                            dataType,
                            isPrimaryKey,
                            position)
                    newColumns.add(newColumn)
                }
            }
        }
        return newColumns
    }

    private fun getDBUser(principalId: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principalId)
        return quote(buildPostgresUsername(securePrincipal))
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

//to get column names by table
//SELECT information_schema.tables.table_name, information_schema.columns.column_name
//FROM information_schema.tables
//LEFT JOIN information_schema.columns on
//information_schema.tables.table_name = information_schema.columns.table_name
//WHERE information_schema.tables.table_schema='public'
//AND table_type='BASE TABLE'
//ORDER BY information_schema.tables.table_name

//to get all relevant information about column data for all tables
//SELECT information_schema.tables.table_name, information_schema.columns.column_name, information_schema.columns.data_type,
// information_schema.columns.ordinal_position, information_schema.table_constraints.constraint_type
// FROM information_schema.tables LEFT JOIN information_schema.columns on information_schema.tables.table_name =
// information_schema.columns.table_name
// LEFT OUTER JOIN information_schema.constraint_column_usage on information_schema.columns.table_name =
// information_schema.constraint_column_usage.table_name
// AND information_schema.columns.column_name = information_schema.constraint_column_usage.column_name
// LEFT OUTER JOIN information_schema.table_constraints on information_schema.constraint_column_usage.constraint_name =
// information_schema.table_constraints.constraint_name
// WHERE information_schema.tables.table_schema='public'
// AND information_schema.tables.table_type='BASE TABLE'
// ORDER BY information_schema.columns.table_name

//to get all relevant column information about a specific table
//SELECT information_schema.tables.table_name, information_schema.columns.column_name, information_schema.columns.data_type,
//information_schema.columns.ordinal_position, information_schema.table_constraints.constraint_type
//FROM information_schema.tables LEFT JOIN information_schema.columns on information_schema.tables.table_name =
//information_schema.columns.table_name
//LEFT OUTER JOIN information_schema.constraint_column_usage on information_schema.columns.table_name =
//information_schema.constraint_column_usage.table_name
//AND information_schema.columns.column_name = information_schema.constraint_column_usage.column_name
//LEFT OUTER JOIN information_schema.table_constraints on information_schema.constraint_column_usage.constraint_name =
//information_schema.table_constraints.constraint_name
//WHERE information_schema.columns.table_name = '$tableName'
//AND information_schema.columns.column_name = '$columnName'