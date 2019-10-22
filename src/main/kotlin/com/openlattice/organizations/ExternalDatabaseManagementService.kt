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

    fun createOrganizationExternalDatabaseTable(orgId: UUID, table: OrganizationExternalDatabaseTable): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        val tableFQN = FullQualifiedName(orgId.toString(), table.name)
        checkState(organizationExternalDatabaseTables.putIfAbsent(table.id, table) == null,
                "OrganizationExternalDatabaseTable ${tableFQN.fullQualifiedNameAsString} already exists")
        aclKeyReservations.reserveIdAndValidateType(table, tableFQN::getFullQualifiedNameAsString)

        val tableAclKey = AclKey(orgId, table.id)
        authorizationManager.setSecurableObjectType(tableAclKey, SecurableObjectType.OrganizationExternalDatabaseTable)
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
        authorizationManager.setSecurableObjectType(columnAclKey, SecurableObjectType.OrganizationExternalDatabaseColumn)
        authorizationManager.addPermission(columnAclKey, principal, EnumSet.allOf(Permission::class.java))

        return column.id
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
     * Sets privileges for a user on an organization's table or column
     */
    fun executePrivilegesUpdate(action: Action, acls: List<Acl>) {
        val aclsByOrg = acls.groupBy { it.aclKey[0] }
        aclsByOrg.forEach { (orgId, acls) ->
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            assemblerConnectionManager.connect(dbName).use { dataSource ->
                dataSource.connection.autoCommit = false
                dataSource.connection.createStatement().use { stmt ->
                    acls.forEach {
                        val tableAndColumnNames = getTableAndColumnNames(AclKey(it.aclKey))
                        it.aces.forEach{ ace ->
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
                            if (action == Action.SET) {
                                val revokeSql = createPrivilegesSql(Action.REMOVE, listOf("ALL"), tableAndColumnNames.first, tableAndColumnNames.second, dbUser)
                                stmt.addBatch(revokeSql)
                            }
                            stmt.addBatch(sql)
                        }
                    }
                    stmt.executeBatch()
                    dataSource.connection.commit()
                }
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
    
    fun getOrganizationIds(): Set<UUID> {
        val orgIds = mutableSetOf<UUID>()
        hds.connection.createStatement().use {
            val rs = it.executeQuery("SELECT id FROM organizations")
            while (rs.next()) {
                orgIds.add(organizationId(rs))
            }
        }
        return orgIds
    }

    private fun getDBUser(principalId: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principalId)
        return quote(buildPostgresUsername(securePrincipal))
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
                    val position = rs.getInt("ordinal_position")
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

    private fun createPrivilegesSql(action: Action, privileges: List<String>, tableName: String, columnName: String, dbUser: String): String {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
        return if (action == Action.REMOVE) {
            "REVOKE $privilegesAsString $columnName ON $tableNamePath FROM $dbUser"
        } else {
            "GRANT $privilegesAsString $columnName ON $tableNamePath TO $dbUser"
        }
    }

    private fun getTableAndColumnNames(aclKey: AclKey): Pair<String, String> {
        val securableObjectType = securableObjectTypes[aclKey]!!
        val tableName: String
        val columnName: String
        val securableObjectId = aclKey.last()
        if (securableObjectType == SecurableObjectType.OrganizationExternalDatabaseColumn) {
            val organizationAtlasColumn = organizationExternalDatabaseColumns[securableObjectId]!!
            tableName = organizationExternalDatabaseTables[organizationAtlasColumn.tableId]!!.name
            columnName = organizationAtlasColumn.name
        } else {
            val organizationAtlasTable = organizationExternalDatabaseTables[securableObjectId]!!
            tableName = organizationAtlasTable.name
            columnName = ""
        }
        //add checks in here for map indexing??
        return Pair(tableName, columnName)
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