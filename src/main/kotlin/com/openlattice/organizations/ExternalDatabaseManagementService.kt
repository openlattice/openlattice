package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManager.Companion.PUBLIC_SCHEMA
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.organizations.mapstores.ID_INDEX
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.DeleteOrganizationExternalDatabaseColumnsEntryProcessor
import com.openlattice.hazelcast.processors.DeleteOrganizationExternalDatabaseTableEntryProcessor
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.organizations.mapstores.TABLE_ID_INDEX
import com.openlattice.organizations.roles.SecurePrincipalsManager

import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresPrivileges
import com.openlattice.postgres.ResultSetAdapters.*
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.OffsetDateTime
import java.util.*

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
    private val organizationTitles: IMap<UUID, String> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATIONS_TITLES.name)
    private val aces: IMap<AceKey, AceValue> = hazelcastInstance.getMap(HazelcastMap.PERMISSIONS.name)
    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)
    private val primaryKeyConstraint = "PRIMARY KEY"
    private val FETCH_SIZE = 100_000

    /*CREATE*/
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
                "OrganizationExternalDatabaseColumn ${column.name} belongs to " +
                        "a table with id ${column.tableId} that does not exist")
        val columnFQN = FullQualifiedName(column.tableId.toString(), column.name)
        checkState(organizationExternalDatabaseColumns.putIfAbsent(column.id, column) == null,
                "OrganizationExternalDatabaseColumn $columnFQN already exists")
        aclKeyReservations.reserveIdAndValidateType(column, columnFQN::getFullQualifiedNameAsString)

        val columnAclKey = AclKey(orgId, column.tableId, column.id)
        authorizationManager.setSecurableObjectType(columnAclKey, SecurableObjectType.OrganizationExternalDatabaseColumn)
        authorizationManager.addPermission(columnAclKey, principal, EnumSet.allOf(Permission::class.java))

        return column.id
    }

    /*GET*/
    fun getOrganizationIds(): Set<UUID> {
        return organizationTitles.keys
    }

    fun getExternalDatabaseTables(orgId: UUID): Set<OrganizationExternalDatabaseTable> {
        return organizationExternalDatabaseTables.values(belongsToOrganization(orgId)).toSet()
    }

    fun getExternalDatabaseTablesWithColumns(orgId: UUID): Map<OrganizationExternalDatabaseTable, Set<OrganizationExternalDatabaseColumn>> {
        val tables = getExternalDatabaseTables(orgId)
        return tables.map {
            it to (organizationExternalDatabaseColumns.values(belongsToTable(it.id)).toSet())
        }.toMap()
    }

    fun getExternalDatabaseTableWithColumns(tableId: UUID): Pair<OrganizationExternalDatabaseTable, Set<OrganizationExternalDatabaseColumn>> {
        val table = getOrganizationExternalDatabaseTable(tableId)
        return Pair(table, (organizationExternalDatabaseColumns.values(belongsToTable(tableId)).toSet()))
    }

    fun getExternalDatabaseTableData(orgId: UUID, tableId: UUID, rowCount: Int): Map<UUID, List<Any?>> {
        val tableName = organizationExternalDatabaseTables.getValue(tableId).name
        val columns = organizationExternalDatabaseColumns.values(belongsToTable(tableId))
        val dataByColumnId = mutableMapOf<UUID, MutableList<Any?>>()

        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val sql = "SELECT * FROM $tableName LIMIT $rowCount"
        BasePostgresIterable(
                StatementHolderSupplier(assemblerConnectionManager.connect(dbName), sql)
        ) { rs ->
            columns.forEach {
                val datum: Any? = rs.getObject(it.name)
                dataByColumnId.getOrPut(it.id) { mutableListOf() }.add(datum)
            }
        }
        return dataByColumnId
    }

    fun getOrganizationExternalDatabaseTable(tableId: UUID): OrganizationExternalDatabaseTable {
        return organizationExternalDatabaseTables.getValue(tableId)
    }

    fun getOrganizationExternalDatabaseColumn(columnId: UUID): OrganizationExternalDatabaseColumn {
        return organizationExternalDatabaseColumns.getValue(columnId)
    }

    /*DELETE*/
    fun deleteOrganizationExternalDatabaseTables(orgId: UUID, tableIds: Set<UUID>) {
        organizationExternalDatabaseTables.executeOnEntries(DeleteOrganizationExternalDatabaseTableEntryProcessor(), idsPredicate(tableIds))
        tableIds.forEach {
            aclKeyReservations.release(it)
            val columnIds = organizationExternalDatabaseColumns.keySet(belongsToTable(it))
            deleteOrganizationExternalDatabaseColumns(orgId, columnIds)
        }
    }

    fun deleteOrganizationExternalDatabaseTable(orgId: UUID, tableId: UUID) {
        deleteOrganizationExternalDatabaseTables(orgId, setOf(tableId))
    }

    fun deleteOrganizationExternalDatabaseColumns(orgId: UUID, columnIds: Set<UUID>) {
        organizationExternalDatabaseColumns.executeOnEntries(DeleteOrganizationExternalDatabaseColumnsEntryProcessor(), idsPredicate(columnIds))
        columnIds.forEach { aclKeyReservations.release(it) }
    }

    fun deleteOrganizationExternalDatabaseColumn(orgId: UUID, columnId: UUID) {
        deleteOrganizationExternalDatabaseColumns(orgId, setOf(columnId))
    }

    /**
     * Deletes an organization's entire database.
     * Is called when an organization is deleted.
     */
    fun deleteOrganizationExternalDatabase(orgId: UUID) {
        //remove all tables/columns within org
        val tableIds = organizationExternalDatabaseTables.keySet(belongsToOrganization(orgId))
        deleteOrganizationExternalDatabaseTables(orgId, tableIds)

        //drop db from schema
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        assemblerConnectionManager.connect(dbName).use {
            it.connection.createStatement().use { stmt ->
                stmt.execute("DROP DATABASE $dbName")
            }
        }
    }

    /*PERMISSIONS*/
    fun addTrustedUser(orgId: UUID, userPrincipal: Principal, ipAdresses: Set<String>) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val userName = getDBUser(userPrincipal.id)
        //edit the pg_hba file through some magic. must. become. magician.
    }

    /**
     * Sets privileges for a user on an organization's table or column
     */
    fun executePrivilegesUpdate(action: Action, acls: List<Acl>) {
        val aclsByOrg = acls.groupBy { it.aclKey[0] }
        aclsByOrg.forEach { (orgId, orgAcls) ->
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            assemblerConnectionManager.connect(dbName).use { dataSource ->
                dataSource.connection.autoCommit = false
                dataSource.connection.createStatement().use { stmt ->
                    orgAcls.forEach {
                        val tableAndColumnNames = getTableAndColumnNames(AclKey(it.aclKey))
                        it.aces.forEach { ace ->
                            if (!areValidPermissions(ace.permissions)) {
                                throw IllegalStateException("Permissions ${ace.permissions} are not valid")
                            }
                            val dbUser = getDBUser(ace.principal.id)
                            val privileges = mutableListOf<String>()

                            //revoke any previous privileges before setting specified ones
                            if (action == Action.SET) {
                                val revokeSql = createPrivilegesSql(Action.REMOVE, listOf("ALL"), tableAndColumnNames.first, tableAndColumnNames.second, dbUser)
                                stmt.addBatch(revokeSql)
                            }

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
                            val grantSql = createPrivilegesSql(action, privileges, tableAndColumnNames.first, tableAndColumnNames.second, dbUser)
                            stmt.addBatch(grantSql)
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

    fun addPermissions(dbName: String, orgId: UUID, tableId: UUID, tableName: String, maybeColumnId: Optional<UUID>, maybeColumnName: Optional<String>) {
        val privilegesByUser = HashMap<String, MutableSet<PostgresPrivileges>>()
        var columnCondition = ""
        lateinit var aclKey: AclKey
        if (maybeColumnId.isPresent && maybeColumnName.isPresent) {
            val columnName = maybeColumnName.get()
            val columnId = maybeColumnId.get()
            columnCondition = "AND column_name = '$columnName'"
            aclKey = AclKey(orgId, tableId, columnId)
        } else {
            aclKey = AclKey(orgId, tableId)
        }
        val sql = "SELECT grantee AS user, privilege_type " +
                "FROM information_schema.role_table_grants " +
                "WHERE table_name = '$tableName' " +
                columnCondition
        BasePostgresIterable(
                StatementHolderSupplier(assemblerConnectionManager.connect(dbName), sql)
        ) { rs ->
            val user = user(rs)
            val privilege = PostgresPrivileges.valueOf(privilegeType(rs).toUpperCase())
            privilegesByUser.getOrPut(user) { mutableSetOf() }.add(privilege)
        }
        privilegesByUser.forEach { (user, privileges) ->
            val principal = securePrincipalsManager.getPrincipal(user).principal
            val aceKey = AceKey(aclKey, principal)
            var permissions = EnumSet.noneOf(Permission::class.java)
            if (privileges == PostgresPrivileges.values().toSet()) {
                permissions.add(Permission.OWNER)
            }
            if (privileges.contains(PostgresPrivileges.SELECT)) {
                permissions.add(Permission.READ)
            }
            if (privileges.contains(PostgresPrivileges.INSERT) || privileges.contains(PostgresPrivileges.UPDATE))
                permissions.add(Permission.WRITE)
            val aceValue = AceValue(permissions, SecurableObjectType.OrganizationExternalDatabaseTable, OffsetDateTime.MAX)
            aces[aceKey] = aceValue //TODO should be a merger or what?
        }
    }

    /*IDK WHERE THESE BELONG CONCEPTUALLY YET*/
    fun getColumnNamesByTable(orgId: UUID, dbName: String): Map<String, Set<String>> {
        val columnNamesByTableName = HashMap<String, MutableSet<String>>()
        val sql = "SELECT information_schema.tables.table_name AS name, information_schema.columns.column_name " +
                "FROM information_schema.tables " +
                "LEFT JOIN information_schema.columns on " +
                "information_schema.tables.table_name = information_schema.columns.table_name " +
                "WHERE information_schema.tables.table_schema='$PUBLIC_SCHEMA' " +
                "AND table_type='BASE TABLE'"
        BasePostgresIterable(
                StatementHolderSupplier(assemblerConnectionManager.connect(dbName), sql, FETCH_SIZE)
        ) { rs -> name(rs) to columnName(rs) }
                .forEach {
                    columnNamesByTableName.getOrPut(it.first) { mutableSetOf() }.add(it.second)
                }
        return columnNamesByTableName
    }

    fun createNewColumnObjects(dbName: String, tableName: String, tableId: UUID, orgId: UUID, columnName: Optional<String>): Set<OrganizationExternalDatabaseColumn> {
        var columnCondition = ""
        columnName.ifPresent { columnName -> columnCondition = "AND information_schema.columns.column_name = '$columnName'" }

        val sql = "SELECT information_schema.tables.table_name, information_schema.columns.column_name, " +
                "information_schema.columns.data_type as datatype, information_schema.columns.ordinal_position, " +
                "information_schema.table_constraints.constraint_type " +
                "FROM information_schema.tables " +
                "LEFT JOIN information_schema.columns on information_schema.tables.table_name = " +
                "information_schema.columns.table_name " +
                "LEFT OUTER JOIN information_schema.constraint_column_usage on " +
                "information_schema.columns.column_name = information_schema.constraint_column_usage.column_name " +
                "AND information_schema.columns.table_name = information_schema.constraint_column_usage.table_name" +
                "LEFT OUTER JOIN information_schema.table_constraints " +
                "on information_schema.constraint_column_usage.constraint_name = " +
                "information_schema.table_constraints.constraint_name " +
                "WHERE information_schema.columns.table_name = '$tableName' " +
                "AND (information_schema.table_constraints.constraint_type = 'PRIMARY KEY' " +
                "OR information_schema.table_constraints.constraint_type IS NULL)" +
                columnCondition
        return BasePostgresIterable(
                StatementHolderSupplier(assemblerConnectionManager.connect(dbName), sql)
        ) { rs ->
            val columnName = columnName(rs)
            val dataType = sqlDataType(rs)
            val position = ordinalPosition(rs)
            val isPrimaryKey = constraintType(rs) == primaryKeyConstraint
            OrganizationExternalDatabaseColumn(
                    Optional.empty(),
                    columnName,
                    columnName,
                    Optional.empty(),
                    tableId,
                    orgId,
                    dataType,
                    isPrimaryKey,
                    position)
        }.toSet()
    }

    /*PRIVATE FUNCTIONS*/
    private fun createPrivilegesSql(action: Action, privileges: List<String>, tableName: String, columnName: String, dbUser: String): String {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        val tableNamePath = "$PUBLIC_SCHEMA.$tableName"
        checkState(action == Action.REMOVE || action == Action.ADD || action == Action.SET,
                "Invalid action $action specified")
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
            val organizationAtlasColumn = organizationExternalDatabaseColumns.getValue(securableObjectId)
            tableName = organizationExternalDatabaseTables.getValue(organizationAtlasColumn.tableId).name
            columnName = organizationAtlasColumn.name
        } else {
            val organizationAtlasTable = organizationExternalDatabaseTables[securableObjectId]!!
            tableName = organizationAtlasTable.name
            columnName = ""
        }
        //add checks in here for map indexing??
        return Pair(tableName, columnName)
    }

    private fun getDBUser(principalId: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principalId)
        return quote(buildPostgresUsername(securePrincipal))
    }

    private fun areValidPermissions(permissions: EnumSet<Permission>): Boolean {
        if (!(permissions.contains(Permission.OWNER) || permissions.contains(Permission.READ) || permissions.contains(Permission.WRITE))) {
            return false
        } else if (permissions.isEmpty()) {
            return false
        }
        return true
    }

    /*INTERNAL SQL QUERIES*/

    /*PREDICATES*/
    private fun idsPredicate(ids: Set<UUID>): Predicate<*, *> {
        return Predicates.`in`(ID_INDEX, *ids.toTypedArray())
    }

    private fun belongsToOrganization(orgId: UUID): Predicate<*, *> {
        return Predicates.equal(ORGANIZATION_ID_INDEX, orgId)
    }

    private fun belongsToTable(tableId: UUID): Predicate<*, *> {
        return Predicates.equal(TABLE_ID_INDEX, tableId)
    }

}