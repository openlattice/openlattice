package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.google.common.collect.Maps
import com.hazelcast.core.HazelcastInstance
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
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.DeleteOrganizationExternalDatabaseColumnsEntryProcessor
import com.openlattice.hazelcast.processors.DeleteOrganizationExternalDatabaseTableEntryProcessor
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseColumnEntryProcessor
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseTableEntryProcessor
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
import kotlin.collections.HashMap

@Service
class ExternalDatabaseManagementService(
        private val hazelcastInstance: HazelcastInstance,
        private val acm: AssemblerConnectionManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val organizationExternalDatabaseConfiguration: OrganizationExternalDatabaseConfiguration,
        private val hds: HikariDataSource
) {

    private val organizationExternalDatabaseColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(hazelcastInstance)
    private val organizationExternalDatabaseTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcastInstance)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)
    private val aces = HazelcastMap.PERMISSIONS.getMap(hazelcastInstance)
    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)
    private val primaryKeyConstraint = "PRIMARY KEY"
    private val FETCH_SIZE = 100_000

    /*CREATE*/
    fun createOrganizationExternalDatabaseTable(orgId: UUID, table: OrganizationExternalDatabaseTable): UUID {
        val tableFQN = FullQualifiedName(orgId.toString(), table.name)
        checkState(organizationExternalDatabaseTables.putIfAbsent(table.id, table) == null,
                "OrganizationExternalDatabaseTable ${tableFQN.fullQualifiedNameAsString} already exists")
        aclKeyReservations.reserveIdAndValidateType(table, tableFQN::getFullQualifiedNameAsString)

        val tableAclKey = AclKey(table.id)
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

        val columnAclKey = AclKey(column.tableId, column.id)
        authorizationManager.setSecurableObjectType(columnAclKey, SecurableObjectType.OrganizationExternalDatabaseColumn)

        return column.id
    }

    fun getColumnMetadata(dbName: String, tableName: String, tableId: UUID, orgId: UUID, columnName: Optional<String>): BasePostgresIterable<
            OrganizationExternalDatabaseColumn> {
        var columnCondition = ""
        columnName.ifPresent { columnCondition = "AND information_schema.columns.column_name = '$it'" }

        val sql = getColumnMetadataSql(tableName, columnCondition)
        return BasePostgresIterable(
                StatementHolderSupplier(acm.connect(dbName), sql)
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

    fun getExternalDatabaseTables(orgId: UUID): Set<Map.Entry<UUID, OrganizationExternalDatabaseTable>> {
        return organizationExternalDatabaseTables.entrySet(belongsToOrganization(orgId))
    }

    fun getExternalDatabaseTablesWithColumns(orgId: UUID): Map<Pair<UUID, OrganizationExternalDatabaseTable>, Set<Map.Entry<UUID, OrganizationExternalDatabaseColumn>>> {
        val tables = getExternalDatabaseTables(orgId)
        return tables.map {
            Pair(it.key, it.value) to (organizationExternalDatabaseColumns.entrySet(belongsToTable(it.key)))
        }.toMap()
    }

    fun getExternalDatabaseTableWithColumns(tableId: UUID): OrganizationExternalDatabaseTableColumnsPair {
        val table = getOrganizationExternalDatabaseTable(tableId)
        return OrganizationExternalDatabaseTableColumnsPair(table, organizationExternalDatabaseColumns.values(belongsToTable(tableId)).toSet())
    }

    fun getExternalDatabaseTableData(
            orgId: UUID,
            tableId: UUID,
            authorizedColumns: Set<OrganizationExternalDatabaseColumn>,
            rowCount: Int): Map<UUID, List<Any?>> {
        val tableName = organizationExternalDatabaseTables.getValue(tableId).name
        val columnNamesSql = authorizedColumns.joinToString(", ") { it.name }
        val dataByColumnId = mutableMapOf<UUID, MutableList<Any?>>()

        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val sql = "SELECT $columnNamesSql FROM $tableName LIMIT $rowCount"
        BasePostgresIterable(
                StatementHolderSupplier(acm.connect(dbName), sql)
        ) { rs ->
            val pairsList = mutableListOf<Pair<UUID, Any?>>()
            authorizedColumns.forEach {
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

    fun getColumnNamesByTable(dbName: String): Map<String, Set<String>> {
        val columnNamesByTableName = Maps.newHashMapWithExpectedSize<String, MutableSet<String>>(organizationExternalDatabaseTables.size)
        val sql = getCurrentTableAndColumnNamesSql()
        BasePostgresIterable(
                StatementHolderSupplier(acm.connect(dbName), sql, FETCH_SIZE)
        ) { rs -> name(rs) to columnName(rs) }
                .forEach {
                    columnNamesByTableName.getOrPut(it.first) { mutableSetOf() }.add(it.second)
                }
        return columnNamesByTableName
    }

    /*UPDATE*/
    fun updateOrganizationExternalDatabaseTable(orgId: UUID, tableFqnToId: Pair<String, UUID>, update: MetadataUpdate) {
        update.name.ifPresent {
            val newTableFqn = FullQualifiedName(orgId.toString(), it)
            val oldTableName = getNameFromFqnString(tableFqnToId.first)
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            acm.connect(dbName).connection.use { conn ->
                val stmt = conn.createStatement()
                stmt.execute("ALTER TABLE $oldTableName RENAME TO $it")
            }
            aclKeyReservations.renameReservation(tableFqnToId.second, newTableFqn.fullQualifiedNameAsString)
        }

        organizationExternalDatabaseTables.submitToKey(tableFqnToId.second, UpdateOrganizationExternalDatabaseTableEntryProcessor(update))
    }

    fun updateOrganizationExternalDatabaseColumn(orgId: UUID, tableFqnToId: Pair<String, UUID>, columnFqnToId: Pair<String, UUID>, update: MetadataUpdate) {
        update.name.ifPresent {
            val tableName = getNameFromFqnString(tableFqnToId.first)
            val newColumnFqn = FullQualifiedName(tableFqnToId.second.toString(), it)
            val oldColumnName = getNameFromFqnString(columnFqnToId.first)
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            acm.connect(dbName).connection.use { conn ->
                val stmt = conn.createStatement()
                stmt.execute("ALTER TABLE $tableName RENAME COLUMN $oldColumnName to $it")
            }
            aclKeyReservations.renameReservation(columnFqnToId.second, newColumnFqn.fullQualifiedNameAsString)
        }
        organizationExternalDatabaseColumns.submitToKey(columnFqnToId.second, UpdateOrganizationExternalDatabaseColumnEntryProcessor(update))
    }

    /*DELETE*/
    fun deleteOrganizationExternalDatabaseTables(orgId: UUID, tableIdByFqn: Map<String, UUID>) {
        tableIdByFqn.forEach { (tableFqn, tableId) ->
            val tableName = getNameFromFqnString(tableFqn)

            //delete columns from tables
            val tableFqnToId = Pair(tableFqn, tableId)
            val columnIdByFqn = organizationExternalDatabaseColumns
                    .entrySet(belongsToTable(tableId))
                    .map { FullQualifiedName(tableId.toString(), it.value.name).toString() to it.key }
                    .toMap()
            val columnsByTable = mapOf(tableFqnToId to columnIdByFqn)
            deleteOrganizationExternalDatabaseColumns(orgId, columnsByTable)

            //delete tables from postgres
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            acm.connect(dbName).connection.use { conn ->
                val stmt = conn.createStatement()
                stmt.execute("DROP TABLE $tableName")
            }
        }

        //delete securable object
        val tableIds = tableIdByFqn.values.toSet()
        deleteOrganizationExternalDatabaseTableObjects(tableIds)
    }

    fun deleteOrganizationExternalDatabaseTableObjects(tableIds: Set<UUID>) {
        tableIds.forEach {
            val aclKey = AclKey(it)
            authorizationManager.deletePermissions(aclKey)
            securableObjectTypes.remove(aclKey)
            aclKeyReservations.release(it)
        }
        organizationExternalDatabaseTables.executeOnEntries(DeleteOrganizationExternalDatabaseTableEntryProcessor(), idsPredicate(tableIds))
    }

    fun deleteOrganizationExternalDatabaseColumns(orgId: UUID, columnsByTable: Map<Pair<String, UUID>, Map<String, UUID>>) {
        columnsByTable.forEach { (tableFqnToId, columnIdsByFqn) ->
            if (columnIdsByFqn.isEmpty()) return@forEach

            val tableName = getNameFromFqnString(tableFqnToId.first)
            val tableId = tableFqnToId.second
            val columnNames = columnIdsByFqn.keys.map { getNameFromFqnString(it) }.toSet()
            val columnIds = columnIdsByFqn.values.toSet()

            //delete columns from postgres
            val dropColumnsSql = createDropColumnSql(columnNames)
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            acm.connect(dbName).connection.use { conn ->
                val stmt = conn.createStatement()
                stmt.execute("ALTER TABLE $tableName $dropColumnsSql")
            }

            deleteOrganizationExternalDatabaseColumnObjects(mapOf(tableId to columnIds))
        }
    }

    fun deleteOrganizationExternalDatabaseColumnObjects(columnIdsByTableId: Map<UUID, Set<UUID>>) {
        columnIdsByTableId.forEach { (tableId, columnIds) ->
            columnIds.forEach {
                val aclKey = AclKey(tableId, it)
                authorizationManager.deletePermissions(aclKey)
                securableObjectTypes.remove(aclKey)
                aclKeyReservations.release(it)
            }
            organizationExternalDatabaseColumns.executeOnEntries(DeleteOrganizationExternalDatabaseColumnsEntryProcessor(), idsPredicate(columnIds))
        }
    }

    /**
     * Deletes an organization's entire database.
     * Is called when an organization is deleted.
     */
    fun deleteOrganizationExternalDatabase(orgId: UUID) {
        //remove all tables/columns within org
        val tableIdByFqn = organizationExternalDatabaseTables
                .entrySet(belongsToOrganization(orgId))
                .map { FullQualifiedName(orgId.toString(), it.value.name).toString() to it.key }
                .toMap()

        deleteOrganizationExternalDatabaseTables(orgId, tableIdByFqn)

        //drop db from schema
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        acm.connect(dbName).connection.use { conn ->
            val stmt = conn.createStatement()
            stmt.execute("DROP DATABASE $dbName")
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
        hds.connection.use { connection ->
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
            val stmt = it.createStatement()
            val hbaTable = PostgresTable.HBA_AUTHENTICATION_RECORDS
            val deleteRecordSql = getDeleteRecordSql(hbaTable, record)
            stmt.executeUpdate(deleteRecordSql)
        }
        updateHBARecords(dbName)
    }

    fun getOrganizationOwners(orgId: UUID): Set<SecurablePrincipal> {
        val principals = securePrincipalsManager.getAuthorizedPrincipalsOnSecurableObject(AclKey(orgId), EnumSet.of(Permission.OWNER))
        return securePrincipalsManager.getSecurablePrincipals(principals).toSet()
    }

    /**
     * Sets privileges for a user on an organization's column
     */
    fun executePrivilegesUpdate(action: Action, columnAcls: List<Acl>) {
        val columnIds = columnAcls.map { it.aclKey[1] }.toSet()
        val columnsById = organizationExternalDatabaseColumns.getAll(columnIds)
        val columnAclsByOrg = columnAcls.groupBy {
            columnsById.getValue(it.aclKey[1]).organizationId
        }

        columnAclsByOrg.forEach { (orgId, columnAcls) ->
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            acm.connect(dbName).connection.use { conn ->
                conn.autoCommit = false
                val stmt = conn.createStatement()
                columnAcls.forEach {
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
                        }
                        val privileges = getPrivilegesFromPermissions(ace.permissions)
                        val grantSql = createPrivilegesUpdateSql(action, privileges, tableName, columnName, dbUser)
                        stmt.addBatch(grantSql)
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
        acm.connect(dbName).connection.use { conn ->
            val stmt = conn.createStatement()
            stmt.execute("REVOKE ALL ON DATABASE $dbName FROM $userName")
        }
    }

    fun syncPermissions(
            dbName: String,
            orgOwnerIds: List<UUID>,
            orgId: UUID,
            tableId: UUID,
            tableName: String,
            maybeColumnId: Optional<UUID>,
            maybeColumnName: Optional<String>
    ): List<Acl> {
        val privilegesByUser = HashMap<UUID, MutableSet<PostgresPrivileges>>()
        val aclKeyUUIDs = mutableListOf(tableId)
        var objectType = SecurableObjectType.OrganizationExternalDatabaseTable
        var aclKey = AclKey(aclKeyUUIDs)
        val ownerPrivileges = PostgresPrivileges.values().toMutableSet()
        ownerPrivileges.remove(PostgresPrivileges.ALL)

        //if column objects, sync postgres privileges
        maybeColumnId.ifPresent { columnId ->
            aclKeyUUIDs.add(columnId)
            aclKey = AclKey(aclKeyUUIDs)
            val privilegesFields = getPrivilegesFields(tableName, maybeColumnName)
            val sql = privilegesFields.first
            objectType = privilegesFields.second


            BasePostgresIterable(
                    StatementHolderSupplier(acm.connect(dbName), sql)
            ) { rs ->
                user(rs) to PostgresPrivileges.valueOf(privilegeType(rs).toUpperCase())
            }
                    .filter { isPostgresUserName(it.first) }
                    .forEach {
                        val securablePrincipalId = getSecurablePrincipalIdFromUserName(it.first)
                        privilegesByUser.getOrPut(securablePrincipalId) { mutableSetOf() }.add(it.second)
                    }
        }

        //give organization owners all privileges
        orgOwnerIds.forEach { orgOwnerId ->
            privilegesByUser.getOrPut(orgOwnerId) { mutableSetOf() }.addAll(ownerPrivileges)
        }

        return privilegesByUser.map { (securablePrincipalId, privileges) ->
            val principal = securePrincipalsManager.getSecurablePrincipalById(securablePrincipalId).principal
            val aceKey = AceKey(aclKey, principal)
            val permissions = EnumSet.noneOf(Permission::class.java)

            if (privileges == ownerPrivileges) {
                permissions.addAll(setOf(Permission.OWNER, Permission.READ, Permission.WRITE))
            } else {
                if (privileges.contains(PostgresPrivileges.SELECT)) {
                    permissions.add(Permission.READ)
                }
                if (privileges.contains(PostgresPrivileges.INSERT) || privileges.contains(PostgresPrivileges.UPDATE))
                    permissions.add(Permission.WRITE)
            }
            aces.executeOnKey(aceKey, PermissionMerger(permissions, objectType, OffsetDateTime.MAX))
            return@map Acl(aclKeyUUIDs, setOf(Ace(principal, permissions, Optional.empty())))
        }

    }

    /*PRIVATE FUNCTIONS*/
    private fun getNameFromFqnString(fqnString: String): String {
        return FullQualifiedName(fqnString).name
    }

    private fun createDropColumnSql(columnNames: Set<String>): String {
        return columnNames.joinToString(", ") { "DROP COLUMN $it" }
    }

    private fun createPrivilegesUpdateSql(action: Action, privileges: List<String>, tableName: String, columnName: String, dbUser: String): String {
        val privilegesAsString = privileges.joinToString(separator = ", ")
        checkState(action == Action.REMOVE || action == Action.ADD || action == Action.SET,
                "Invalid action $action specified")
        return if (action == Action.REMOVE) {
            "REVOKE $privilegesAsString ($columnName) ON $tableName FROM $dbUser"
        } else {
            "GRANT $privilegesAsString ($columnName) ON $tableName TO $dbUser"
        }
    }

    private fun getTableAndColumnNames(aclKey: AclKey): Pair<String, String> {
        val securableObjectId = aclKey[1]
        val organizationAtlasColumn = organizationExternalDatabaseColumns.getValue(securableObjectId)
        val tableName = organizationExternalDatabaseTables.getValue(organizationAtlasColumn.tableId).name
        val columnName = organizationAtlasColumn.name
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
            acm.connect(dbName).connection.use { conn ->
                val stmt = conn.createStatement()
                stmt.executeQuery(getReloadConfigSql())
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