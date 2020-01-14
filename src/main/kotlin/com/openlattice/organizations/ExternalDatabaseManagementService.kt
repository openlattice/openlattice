package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManager.Companion.PUBLIC_SCHEMA
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.assembler.PostgresRoles.Companion.getSecurablePrincipalIdFromUserName
import com.openlattice.assembler.PostgresRoles.Companion.isPostgresUserName
import com.openlattice.authorization.*
import com.openlattice.authorization.processors.PermissionMerger
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.DeleteOrganizationExternalDatabaseColumnsEntryProcessor
import com.openlattice.hazelcast.processors.DeleteOrganizationExternalDatabaseTableEntryProcessor
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.OrganizationExternalDatabaseTableColumnsPair
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.organizations.mapstores.TABLE_ID_INDEX
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.*

import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.ResultSetAdapters.*
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.BufferedOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.OffsetDateTime
import java.util.*

@Service
class ExternalDatabaseManagementService(
        private val hazelcastInstance: HazelcastInstance,
        private val assemblerConnectionManager: AssemblerConnectionManager, //for now using this, may need to move connection logic to its own file
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val organizationExternalDatabaseConfiguration: OrganizationExternalDatabaseConfiguration,
        private val hds: HikariDataSource
) {

    private val organizationExternalDatabaseColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap( hazelcastInstance )
    private val organizationExternalDatabaseTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap( hazelcastInstance )
    private val hbaAuthenticationRecordsMapstore = HazelcastMap.HBA_AUTHENTICATION_RECORDS.getMap( hazelcastInstance )
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap( hazelcastInstance )
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap( hazelcastInstance )
    private val aces = HazelcastMap.PERMISSIONS.getMap( hazelcastInstance )
    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)
    private val primaryKeyConstraint = "PRIMARY KEY"
    private val FETCH_SIZE = 100_000

    /*CREATE*/
    fun createOrganizationExternalDatabaseTable(orgId: UUID, table: OrganizationExternalDatabaseTable): UUID {
        val tableFQN = FullQualifiedName(orgId.toString(), table.name)
        checkState(organizationExternalDatabaseTables.putIfAbsent(table.id, table) == null,
                "OrganizationExternalDatabaseTable ${tableFQN.fullQualifiedNameAsString} already exists")
        aclKeyReservations.reserveIdAndValidateType(table, tableFQN::getFullQualifiedNameAsString)

        val tableAclKey = AclKey(orgId, table.id)
        authorizationManager.setSecurableObjectType(tableAclKey, SecurableObjectType.OrganizationExternalDatabaseTable)

        return table.id
    }

    fun createOrganizationExternalDatabaseColumn(orgId: UUID, column: OrganizationExternalDatabaseColumn): UUID {
        checkState(organizationExternalDatabaseTables[column.tableId] != null,
                "OrganizationExternalDatabaseColumn ${column.name} belongs to " +
                        "a table with id ${column.tableId} that does not exist")
        val columnFQN = FullQualifiedName(column.tableId.toString(), column.name)
        checkState(organizationExternalDatabaseColumns.putIfAbsent(column.id, column) == null,
                "OrganizationExternalDatabaseColumn $columnFQN already exists")
        aclKeyReservations.reserveIdAndValidateType(column, columnFQN::getFullQualifiedNameAsString)

        val columnAclKey = AclKey(orgId, column.tableId, column.id)
        authorizationManager.setSecurableObjectType(columnAclKey, SecurableObjectType.OrganizationExternalDatabaseColumn)

        return column.id
    }

    fun createNewColumnObjects(dbName: String, tableName: String, tableId: UUID, orgId: UUID, columnName: Optional<String>): BasePostgresIterable<OrganizationExternalDatabaseColumn> {
        var columnCondition = ""
        columnName.ifPresent { columnCondition = "AND information_schema.columns.column_name = '$it'" }

        val sql = getColumnMetadataSql(tableName, columnCondition)
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
        }
    }

    /*GET*/
    fun getOrganizationIds(): Set<UUID> {
        return organizations.keys
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

    fun getExternalDatabaseTableWithColumns(tableId: UUID): OrganizationExternalDatabaseTableColumnsPair {
        val table = getOrganizationExternalDatabaseTable(tableId)
        return OrganizationExternalDatabaseTableColumnsPair(table, organizationExternalDatabaseColumns.values(belongsToTable(tableId)).toSet())
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
            val pairsList = mutableListOf<Pair<UUID, Any?>>()
            columns.forEach {
                pairsList.add(it.id to rs.getObject(it.name))
            }
            return@BasePostgresIterable pairsList
        }.forEach { pairsList ->
            pairsList.forEach {
                dataByColumnId.getOrPut(it.first) { mutableListOf() }.add(it.second)
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

    fun getColumnNamesByTable(orgId: UUID, dbName: String): Map<String, Set<String>> {
        val columnNamesByTableName = HashMap<String, MutableSet<String>>()
        val sql = getCurrentTableAndColumnNamesSql()
        BasePostgresIterable(
                StatementHolderSupplier(assemblerConnectionManager.connect(dbName), sql, FETCH_SIZE)
        ) { rs -> name(rs) to columnName(rs) }
                .forEach {
                    columnNamesByTableName.getOrPut(it.first) { mutableSetOf() }.add(it.second)
                }
        return columnNamesByTableName
    }

    /*DELETE*/
    fun deleteOrganizationExternalDatabaseTables(orgId: UUID, tableIds: Set<UUID>) {
        organizationExternalDatabaseTables.executeOnEntries(DeleteOrganizationExternalDatabaseTableEntryProcessor(), idsPredicate(tableIds))
        tableIds.forEach {
            val aclKey = AclKey(orgId, it)
            authorizationManager.deletePermissions(aclKey)
            securableObjectTypes.remove(aclKey)
            aclKeyReservations.release(it)
            val columnIdsByTableId = mapOf(it to organizationExternalDatabaseColumns.keySet(belongsToTable(it)))
            deleteOrganizationExternalDatabaseColumns(orgId, columnIdsByTableId)
        }
    }

    fun deleteOrganizationExternalDatabaseTable(orgId: UUID, tableId: UUID) {
        deleteOrganizationExternalDatabaseTables(orgId, setOf(tableId))
    }

    fun deleteOrganizationExternalDatabaseColumns(orgId: UUID, columnIdsByTableId: Map<UUID, Set<UUID>>) {
        columnIdsByTableId.forEach { (tableId, columnIds) ->
            organizationExternalDatabaseColumns.executeOnEntries(DeleteOrganizationExternalDatabaseColumnsEntryProcessor(), idsPredicate(columnIds))
            columnIds.forEach {
                val aclKey = AclKey(orgId, tableId, it)
                authorizationManager.deletePermissions(aclKey)
                securableObjectTypes.remove(aclKey)
                aclKeyReservations.release(it)
            }
        }
    }

    fun deleteOrganizationExternalDatabaseColumn(orgId: UUID, tableId: UUID, columnId: UUID) {
        deleteOrganizationExternalDatabaseColumns(orgId, mapOf(tableId to setOf(columnId)))
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
    fun addHBARecord(orgId: UUID, userPrincipal: Principal, connectionType: PostgresConnectionType, ipAddress: String) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val username = getDBUser(userPrincipal.id)
        val record = PostgresAuthenticationRecord(
                connectionType,
                dbName,
                username,
                ipAddress,
                organizationExternalDatabaseConfiguration.authMethod)
        hds.connection.use {connection ->
            connection.createStatement().use { stmt ->
                val hbaTable = PostgresTable.HBA_AUTHENTICATION_RECORDS
                val columns = hbaTable.columns.joinToString(", ", "(", ")") { it.name }
                val pkey = hbaTable.primaryKey.joinToString(", ", "(", ")") { it.name }
                val insertRecordSql = getInsertRecordSql(hbaTable, columns, pkey, record)
                stmt.executeUpdate(insertRecordSql)
            }
        }
        updateHBARecords(dbName)
    }

    fun removeHBARecord(orgId: UUID, userPrincipal: Principal, connectionType: PostgresConnectionType, ipAddress: String) {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val username = getDBUser(userPrincipal.id)
        val record = PostgresAuthenticationRecord(
                connectionType,
                dbName,
                username,
                ipAddress,
                organizationExternalDatabaseConfiguration.authMethod)
        hds.connection.use {
            it.createStatement().use { stmt ->
                val hbaTable = PostgresTable.HBA_AUTHENTICATION_RECORDS
                val deleteRecordSql = getDeleteRecordSql(hbaTable, record)
                stmt.executeUpdate(deleteRecordSql)
            }
        }
        updateHBARecords(dbName)
    }

    /**
     * Sets privileges for a user on an organization's table or column
     */
    fun executePrivilegesUpdate(action: Action, acls: List<Acl>) {
        val aclsByOrg = acls.groupBy { it.aclKey[0] }
        aclsByOrg.forEach { (orgId, orgAcls) ->
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            assemblerConnectionManager.connect(dbName).use { dataSource ->
                val conn = dataSource.connection
                conn.autoCommit = false
                conn.createStatement().use { stmt ->
                    dataSource.connection.autoCommit = false
                    orgAcls.forEach {
                        val aclKey = AclKey(it.aclKey)
                        val tableAndColumnNames = getTableAndColumnNames(AclKey(it.aclKey))
                        val tableName = tableAndColumnNames.first
                        val columnName = tableAndColumnNames.second
                        it.aces.forEach { ace ->
                            if (!areValidPermissions(ace.permissions)) {
                                throw IllegalStateException("Permissions ${ace.permissions} are not valid")
                            }
                            val dbUser = getDBUser(ace.principal.id)

                            //revoke any previous privileges before setting specified ones
                            if (action == Action.SET) {
                                val revokeSql = createPrivilegesUpdateSql(Action.REMOVE, listOf("ALL"), tableName, columnName, dbUser)
                                stmt.addBatch(revokeSql)

                                //maintain column-level privileges if object is a table
                                if (columnName.isEmpty) {
                                    getMaintainColumnPrivilegesSql(aclKey, ace.principal, tableName, dbUser).forEach { maintenanceStmt ->
                                        stmt.addBatch(maintenanceStmt)
                                    }
                                }
                            }
                            val privileges = getPrivilegesFromPermissions(ace.permissions)
                            val grantSql = createPrivilegesUpdateSql(action, privileges, tableName, columnName, dbUser)
                            stmt.addBatch(grantSql)

                            //if we've removed privileges on a table, maintain column-level privileges
                            if (action == Action.REMOVE) {
                                if (columnName.isEmpty) {
                                    getMaintainColumnPrivilegesSql(aclKey, ace.principal, tableName, dbUser).forEach { maintenanceStmt ->
                                        stmt.addBatch(maintenanceStmt)
                                    }
                                }
                            }
                        }
                    }
                    stmt.executeBatch()
                    conn.commit()
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
        val privilegesByUser = HashMap<UUID, MutableSet<PostgresPrivileges>>()
        val aclKeyUUIDs = mutableListOf(orgId, tableId)
        maybeColumnId.ifPresent { aclKeyUUIDs.add(it) }
        val aclKey = AclKey(aclKeyUUIDs)
        val privilegesFields = getPrivilegesFields(tableName, maybeColumnName)
        val sql = privilegesFields.first
        val objectType = privilegesFields.second

        BasePostgresIterable(
                StatementHolderSupplier(assemblerConnectionManager.connect(dbName), sql)
        ) { rs ->
            user(rs) to PostgresPrivileges.valueOf(privilegeType(rs).toUpperCase())
        }
                .filter { isPostgresUserName(it.first) }
                .forEach {
                    val securablePrincipalId = getSecurablePrincipalIdFromUserName(it.first)
                    privilegesByUser.getOrPut(securablePrincipalId) { mutableSetOf() }.add(it.second)
                }

        privilegesByUser.forEach { (securablePrincipalId, privileges) ->
            val principal = securePrincipalsManager.getSecurablePrincipalById(securablePrincipalId).principal
            val aceKey = AceKey(aclKey, principal)
            val permissions = EnumSet.noneOf(Permission::class.java)
            val ownerPrivileges = PostgresPrivileges.values().toMutableSet()
            ownerPrivileges.remove(PostgresPrivileges.ALL)
            if (privileges == ownerPrivileges) {
                permissions.add(Permission.OWNER)
            }
            if (privileges.contains(PostgresPrivileges.SELECT)) {
                permissions.add(Permission.READ)
            }
            if (privileges.contains(PostgresPrivileges.INSERT) || privileges.contains(PostgresPrivileges.UPDATE))
                permissions.add(Permission.WRITE)
            aces.executeOnKey(aceKey, PermissionMerger(permissions, objectType, OffsetDateTime.MAX))
        }
    }

    /*PRIVATE FUNCTIONS*/
    private fun createPrivilegesUpdateSql(action: Action, privileges: List<String>, tableName: String, columnName: Optional<String>, dbUser: String): String {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        checkState(action == Action.REMOVE || action == Action.ADD || action == Action.SET,
                "Invalid action $action specified")
        var columnField = ""
        columnName.ifPresent { columnField = "($it)" }
        return if (action == Action.REMOVE) {
            "REVOKE $privilegesAsString $columnField ON $tableName FROM $dbUser"
        } else {
            "GRANT $privilegesAsString $columnField ON $tableName TO $dbUser"
        }
    }

    private fun getTableAndColumnNames(aclKey: AclKey): Pair<String, Optional<String>> {
        val securableObjectType = securableObjectTypes[aclKey]!!
        val tableName: String
        val columnName: Optional<String>
        val securableObjectId = aclKey.last()
        if (securableObjectType == SecurableObjectType.OrganizationExternalDatabaseColumn) {
            val organizationAtlasColumn = organizationExternalDatabaseColumns.getValue(securableObjectId)
            tableName = organizationExternalDatabaseTables.getValue(organizationAtlasColumn.tableId).name
            columnName = Optional.of(organizationAtlasColumn.name)
        } else {
            val organizationAtlasTable = organizationExternalDatabaseTables.getValue(securableObjectId)
            tableName = organizationAtlasTable.name
            columnName = Optional.empty()
        }
        return Pair(tableName, columnName)
    }

    private fun getDBUser(principalId: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principalId)
        checkState(securePrincipal.principalType == PrincipalType.USER, "Principal must be of type USER")
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

    private fun getMaintainColumnPrivilegesSql(aclKey: AclKey, principal: Principal, tableName: String, dbUser: String): List<String> {
        val maintenanceStmts = mutableListOf<String>()
        organizationExternalDatabaseColumns.values(belongsToTable(aclKey.last())).forEach {
            val columnAclKey = AclKey(listOf(aclKey[0], aclKey[1], it.id))
            val columnAceKey = AceKey(columnAclKey, principal)
            val columnAceValue = aces[columnAceKey]
            if (columnAceValue != null) {
                val columnPrivileges = getPrivilegesFromPermissions(columnAceValue.permissions)
                val grantSql = createPrivilegesUpdateSql(Action.ADD, columnPrivileges, tableName, Optional.of(it.name), dbUser)
                maintenanceStmts.add(grantSql)
            }
        }
        return maintenanceStmts
    }

    private fun getPrivilegesFromPermissions(permissions: EnumSet<Permission>): List<String> {
        val privileges = mutableListOf<String>()
        if (permissions.contains(Permission.OWNER)) {
            privileges.add(PostgresPrivileges.ALL.toString())
        } else {
            if (permissions.contains(Permission.WRITE)) {
                privileges.addAll(listOf(
                        PostgresPrivileges.INSERT.toString(),
                        PostgresPrivileges.UPDATE.toString()))
            }
            if (permissions.contains(Permission.READ)) {
                privileges.add(PostgresPrivileges.SELECT.toString())
            }
        }
        return privileges
    }

    private fun getPrivilegesFields(tableName: String, maybeColumnName: Optional<String>): Pair<String, SecurableObjectType> {
        var columnCondition = ""
        var grantsTableName = "information_schema.role_table_grants"
        var objectType = SecurableObjectType.OrganizationExternalDatabaseTable
        if (maybeColumnName.isPresent) {
            val columnName = maybeColumnName.get()
            columnCondition = "AND column_name = '$columnName'"
            grantsTableName = "information_schema.role_column_grants"
            objectType = SecurableObjectType.OrganizationExternalDatabaseColumn
        }
        val sql = getCurrentUsersPrivilegesSql(tableName, grantsTableName, columnCondition)
        return Pair(sql, objectType)
    }

    private fun updateHBARecords(dbName: String) {
        val originalHBAPath = Paths.get(organizationExternalDatabaseConfiguration.path + organizationExternalDatabaseConfiguration.fileName)
        val tempHBAPath = Paths.get(organizationExternalDatabaseConfiguration.path + "/temp_hba.conf")

        //create hba file with new records
        val records = getHBARecords(PostgresTable.HBA_AUTHENTICATION_RECORDS.name)
                .map {
                    it.buildHBAConfRecord()
                }.toSet()
        try {
            val out = BufferedOutputStream(
                    Files.newOutputStream(tempHBAPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING)
            )
            records.forEach {
                val recordAsByteArray = it.toByteArray()
                out.write(recordAsByteArray)
            }
            out.close()

            //reload config
            assemblerConnectionManager.connect(dbName).use {
                it.connection.createStatement().use { stmt ->
                    stmt.executeQuery(getReloadConfigSql())
                }
            }
        } catch (ex: IOException) {
            logger.info("IO exception while creating new hba config")
        }

        //replace old hba with new hba
        try {
            Files.move(tempHBAPath, originalHBAPath, StandardCopyOption.REPLACE_EXISTING)
        } catch (ex: IOException) {
            logger.info("IO exception while updating hba config")
        }

    }

    private fun getHBARecords(tableName: String): BasePostgresIterable<PostgresAuthenticationRecord> {
        return BasePostgresIterable(
                StatementHolderSupplier(hds, getSelectRecordsSql(tableName))
        ) { rs ->
            postgresAuthenticationRecord(rs)
        }
    }

    /*INTERNAL SQL QUERIES*/
    private fun getCurrentTableAndColumnNamesSql(): String {
        return selectExpression + fromExpression + leftJoinColumnsExpression +
                "WHERE information_schema.tables.table_schema='$PUBLIC_SCHEMA' " +
                "AND table_type='BASE TABLE'"
    }

    private fun getColumnMetadataSql(tableName: String, columnCondition: String): String {
        return selectExpression + ", information_schema.columns.data_type AS datatype, " +
                "information_schema.columns.ordinal_position, " +
                "information_schema.table_constraints.constraint_type " +
                fromExpression + leftJoinColumnsExpression +
                "LEFT OUTER JOIN information_schema.constraint_column_usage ON " +
                "information_schema.columns.column_name = information_schema.constraint_column_usage.column_name " +
                "AND information_schema.columns.table_name = information_schema.constraint_column_usage.table_name " +
                "LEFT OUTER JOIN information_schema.table_constraints " +
                "ON information_schema.constraint_column_usage.constraint_name = " +
                "information_schema.table_constraints.constraint_name " +
                "WHERE information_schema.columns.table_name = '$tableName' " +
                "AND (information_schema.table_constraints.constraint_type = 'PRIMARY KEY' " +
                "OR information_schema.table_constraints.constraint_type IS NULL)" +
                columnCondition
    }

    private fun getCurrentUsersPrivilegesSql(tableName: String, grantsTableName: String, columnCondition: String): String {
        return "SELECT grantee AS user, privilege_type " +
                "FROM $grantsTableName " +
                "WHERE table_name = '$tableName' " +
                columnCondition
    }

    private fun getReloadConfigSql(): String {
        return "SELECT pg_reload_conf()"
    }

    private val selectExpression = "SELECT information_schema.tables.table_name AS name, information_schema.columns.column_name "

    private val fromExpression = "FROM information_schema.tables "

    private val leftJoinColumnsExpression = "LEFT JOIN information_schema.columns ON information_schema.tables.table_name = information_schema.columns.table_name "

    private fun getInsertRecordSql(table: PostgresTableDefinition, columns: String, pkey: String, record: PostgresAuthenticationRecord): String {
        return "INSERT INTO ${table.name} $columns VALUES(${record.buildPostgresRecord()}) " +
                "ON CONFLICT $pkey DO UPDATE SET ${PostgresColumn.AUTHENTICATION_METHOD.name}=EXCLUDED.${PostgresColumn.AUTHENTICATION_METHOD.name}"
    }

    private fun getDeleteRecordSql(table: PostgresTableDefinition, record: PostgresAuthenticationRecord): String {
        return "DELETE FROM ${table.name} WHERE ${PostgresColumn.USERNAME.name} = '${record.username}' " +
                "AND ${PostgresColumn.DATABASE.name} = '${record.database}' " +
                "AND ${PostgresColumn.CONNECTION_TYPE.name} = '${record.connectionType}' " +
                "AND ${PostgresColumn.IP_ADDRESS.name} = '${record.ipAddress}'"
    }

    private fun getSelectRecordsSql(tableName: String): String {
        return "SELECT * FROM $tableName"
    }

    /*PREDICATES*/
    private fun idsPredicate(ids: Set<UUID>): Predicate<*, *> {
        return Predicates.`in`(QueryConstants.KEY_ATTRIBUTE_NAME.value(), *ids.toTypedArray())
    }

    private fun belongsToOrganization(orgId: UUID): Predicate<*, *> {
        return Predicates.equal(ORGANIZATION_ID_INDEX, orgId)
    }

    private fun belongsToTable(tableId: UUID): Predicate<*, *> {
        return Predicates.equal(TABLE_ID_INDEX, tableId)
    }

}