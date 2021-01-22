package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.AssemblerConnectionManager.Companion.OPENLATTICE_SCHEMA
import com.openlattice.assembler.AssemblerConnectionManager.Companion.STAGING_SCHEMA
import com.openlattice.assembler.dropAllConnectionsToDatabaseSql
import com.openlattice.authorization.*
import com.openlattice.authorization.processors.PermissionMerger
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetFqnFromPropertyTypeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseColumnEntryProcessor
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseTableEntryProcessor
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.OrganizationExternalDatabaseTableColumnsPair
import com.openlattice.organizations.mapstores.*
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.ResultSetAdapters.*
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.openlattice.transporter.processors.DestroyTransportedEntitySetEntryProcessor
import com.openlattice.transporter.processors.GetPropertyTypesFromTransporterColumnSetEntryProcessor
import com.openlattice.transporter.processors.TransportEntitySetEntryProcessor
import com.openlattice.transporter.types.TransporterDatastore
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.BufferedOutputStream
import java.io.IOException
import java.lang.IllegalArgumentException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.OffsetDateTime
import java.util.*
import kotlin.streams.toList

@Service
class ExternalDatabaseManagementService(
        hazelcastInstance: HazelcastInstance,
        private val externalDbManager: ExternalDatabaseConnectionManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val organizationExternalDatabaseConfiguration: OrganizationExternalDatabaseConfiguration,
        private val transporterDatastore: TransporterDatastore,
        private val dbCredentialService: DbCredentialService,
        private val hds: HikariDataSource
) {

    private val organizationExternalDatabaseColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(hazelcastInstance)
    private val organizationExternalDatabaseTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcastInstance)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val aces = HazelcastMap.PERMISSIONS.getMap(hazelcastInstance)
    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)
    private val primaryKeyConstraint = "PRIMARY KEY"
    private val FETCH_SIZE = 100_000

    /**
     * Only needed for materialize entity set, which should move elsewhere eventually
     */
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)
    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap(hazelcastInstance)

    companion object {
        private val OWNER_PRIVILEGES = PostgresPrivileges.values().toSet() - PostgresPrivileges.ALL
    }

    /*CREATE*/
    fun createOrganizationExternalDatabaseTable(orgId: UUID, table: OrganizationExternalDatabaseTable): UUID {
        val tableUniqueName = table.getUniqueName()
        checkState(organizationExternalDatabaseTables.putIfAbsent(table.id, table) == null,
                "OrganizationExternalDatabaseTable $tableUniqueName already exists")
        aclKeyReservations.reserveIdAndValidateType(table) { tableUniqueName }

        val tableAclKey = AclKey(table.id)
        authorizationManager.setSecurableObjectType(tableAclKey, SecurableObjectType.OrganizationExternalDatabaseTable)

        return table.id
    }

    fun createOrganizationExternalDatabaseColumn(orgId: UUID, column: OrganizationExternalDatabaseColumn): UUID {
        checkState(organizationExternalDatabaseTables[column.tableId] != null,
                "OrganizationExternalDatabaseColumn ${column.name} belongs to " +
                        "a table with id ${column.tableId} that does not exist")
        val columnUniqueName = column.getUniqueName()
        checkState(organizationExternalDatabaseColumns.putIfAbsent(column.id, column) == null,
                "OrganizationExternalDatabaseColumn $columnUniqueName already exists")
        aclKeyReservations.reserveIdAndValidateType(column) { columnUniqueName }

        authorizationManager.setSecurableObjectType(column.getAclKey(), SecurableObjectType.OrganizationExternalDatabaseColumn)

        return column.id
    }

    fun getColumnMetadata(
            tableName: String,
            tableId: UUID,
            orgId: UUID,
            columnName: Optional<String> = Optional.empty()
    ): List<OrganizationExternalDatabaseColumn> {
        var columnCondition = ""
        columnName.ifPresent { columnCondition = "AND information_schema.columns.column_name = '$it'" }

        val sql = getColumnMetadataSql(tableName, columnCondition)
        return BasePostgresIterable(
                StatementHolderSupplier(externalDbManager.connectToOrg(orgId), sql)
        ) { rs ->
            try {
                val storedColumnName = columnName(rs)
                val dataType = sqlDataType(rs)
                val position = ordinalPosition(rs)
                val isPrimaryKey = constraintType(rs) == primaryKeyConstraint
                OrganizationExternalDatabaseColumn(
                        Optional.empty(),
                        storedColumnName,
                        storedColumnName,
                        Optional.empty(),
                        tableId,
                        orgId,
                        dataType,
                        isPrimaryKey,
                        position)
            } catch (e: Exception) {
                logger.error("Unable to map column to OrganizationExternalDatabaseColumn object for table {} in organization {}", tableName, orgId, e)
                null
            }
        }.filterNotNull()
    }

    fun destroyTransportedEntitySet(entitySetId: UUID) {
        entitySets.executeOnKey(entitySetId, DestroyTransportedEntitySetEntryProcessor().init(transporterDatastore))
    }

    fun transportEntitySet(organizationId: UUID, entitySetId: UUID) {
        val memberPrincipals = securePrincipalsManager.getOrganizationMembers(setOf(organizationId)).getValue(organizationId)

        val userToPrincipals = securePrincipalsManager.bulkGetUnderlyingPrincipals(memberPrincipals).mapKeys {
            dbCredentialService.getDbUsername(it.key)
        }

        val entityTypeId = entitySets.submitToKey(
                entitySetId,
                GetEntityTypeFromEntitySetEntryProcessor()
        )

        val accessCheckCompletion = entityTypeId.thenCompose { etid ->
            entityTypes.getAsync(etid!!)
        }.thenApplyAsync { entityType ->
            entityType.properties.mapTo(mutableSetOf()) { ptid ->
                AccessCheck(AclKey(entitySetId, ptid), EnumSet.of(Permission.READ))
            }
        }

        val transporterColumnsCompletion = entityTypeId.thenCompose { etid ->
            requireNotNull(etid) {
                "Entity set $entitySetId has no entity type"
            }
            transporterState.submitToKey(etid, GetPropertyTypesFromTransporterColumnSetEntryProcessor())
        }.thenCompose { transporterPtIds ->
            propertyTypes.submitToKeys(transporterPtIds, GetFqnFromPropertyTypeEntryProcessor())
        }

        val userToPermissionsCompletion = accessCheckCompletion.thenApply { accessChecks ->
            userToPrincipals.mapValues { (_, principals) ->
                authorizationManager.accessChecksForPrincipals(accessChecks, principals).filter {
                    it.permissions[Permission.READ]!!
                }.map {
                    it.aclKey[1]
                }.toList()
            }
        }

        userToPermissionsCompletion.thenCombine(transporterColumnsCompletion) { userToPtCols, transporterColumns ->
            val userToEntitySetColumnNames = userToPtCols.mapValues { (_, columns) ->
                columns.map {
                    transporterColumns.get(it).toString()
                }
            }
            entitySets.submitToKey(entitySetId,
                    TransportEntitySetEntryProcessor(transporterColumns, organizationId, userToEntitySetColumnNames)
                            .init(transporterDatastore)
            )
        }.toCompletableFuture().get().toCompletableFuture().get()
    }

    /*GET*/

    fun getExternalDatabaseTables(orgId: UUID): Map<UUID, OrganizationExternalDatabaseTable> {
        return organizationExternalDatabaseTables.values(belongsToOrganization(orgId)).associateBy { it.id }
    }

    /**
     * Returns a map from tableId to its columns, as a map from columnId to column
     */
    fun getColumnsForTables(tableIds: Set<UUID>): Map<UUID, Map<UUID, OrganizationExternalDatabaseColumn>> {
        return organizationExternalDatabaseColumns
                .values(belongsToTables(tableIds))
                .groupBy { it.tableId }
                .mapValues { it.value.associateBy { c -> c.id } }
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

        val sql = "SELECT $columnNamesSql FROM $tableName LIMIT $rowCount"
        BasePostgresIterable(
                StatementHolderSupplier(externalDbManager.connectToOrg(orgId), sql)
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

    fun getColumnNamesByTableName(dbName: String): List<TableInfo> {
        return BasePostgresIterable(StatementHolderSupplier(
                externalDbManager.connect(dbName),
                getCurrentTableAndColumnNamesSql(),
                FETCH_SIZE
        )) { rs ->
            TableInfo(oid(rs), name(rs), schemaName(rs), columnNames(rs))
        }.toList()
    }

    /*UPDATE*/
    fun updateOrganizationExternalDatabaseTable(orgId: UUID, tableFqnToId: Pair<String, UUID>, update: MetadataUpdate) {
        update.name.ifPresent {
            val newTableFqn = FullQualifiedName(orgId.toString(), it)
            val oldTableName = getNameFromFqnString(tableFqnToId.first)
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
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
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
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
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
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
        organizationExternalDatabaseTables.removeAll(idsPredicate(tableIds))
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
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
                val stmt = conn.createStatement()
                stmt.execute("ALTER TABLE $tableName $dropColumnsSql")
            }

            deleteOrganizationExternalDatabaseColumnObjects(mapOf(tableId to columnIds))
        }
    }

    fun deleteOrganizationExternalDatabaseColumnObjects(columnIdsByTableId: Map<UUID, Set<UUID>>) {
        columnIdsByTableId.forEach { (tableId, columnIds) ->
            columnIds.forEach { columnId ->
                val aclKey = AclKey(tableId, columnId)
                authorizationManager.deletePermissions(aclKey)
                securableObjectTypes.remove(aclKey)
                aclKeyReservations.release(columnId)
            }
            organizationExternalDatabaseColumns.removeAll(idsPredicate(columnIds))
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
        val dbName = externalDbManager.getOrganizationDatabaseName(orgId)
        externalDbManager.connectAsSuperuser().connection.use { conn ->
            val stmt = conn.createStatement()
            stmt.execute(dropAllConnectionsToDatabaseSql(dbName))
            stmt.execute("DROP DATABASE $dbName")
        }


        externalDbManager.deleteOrganizationDatabase(orgId)
    }

    /*PERMISSIONS*/
    fun addHBARecord(orgId: UUID, userPrincipal: Principal, connectionType: PostgresConnectionType, ipAddress: String) {
        val dbName = externalDbManager.getOrganizationDatabaseName(orgId)
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
        val dbName = externalDbManager.getOrganizationDatabaseName(orgId)
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
    fun executePrivilegesUpdate(action: Action, ungroupedColumnAcls: List<Acl>) {
        ensurePermissionsAreValid(ungroupedColumnAcls)

        val columnIds = ungroupedColumnAcls.map { it.aclKey[1] }.toSet()
        val columnsById = organizationExternalDatabaseColumns.getAll(columnIds)
        val columnAclsByOrg = ungroupedColumnAcls.groupBy {
            columnsById.getValue(it.aclKey[1]).organizationId
        }

        val usernamesByPrincipal = getDbUsers(ungroupedColumnAcls.flatMap { it.aces.map { ace -> ace.principal } })

        columnAclsByOrg.forEach { (orgId, columnAclsByTable) ->
            val columnAclsByTableId = columnAclsByTable.groupBy { it.aclKey.first() }

            val tables = organizationExternalDatabaseTables.getAll(columnAclsByTableId.keys)

            externalDbManager.connectToOrg(orgId).connection.use { conn ->
                conn.autoCommit = false

                columnAclsByTableId
                        .mapNotNull { (tableId, columnAcls) -> tables[tableId]?.let { it to columnAcls } }
                        .forEach { (table, columnAcls) ->

                            columnAcls
                                    .mapNotNull { acl -> columnsById[acl.aclKey[1]]?.let { it.name to acl.aces } }
                                    .forEach { (columnName, aces) ->

                                        val updateStatements = getUdpateStatementsForAces(
                                                action,
                                                table,
                                                columnName,
                                                aces,
                                                usernamesByPrincipal
                                        )

                                        conn.createStatement().use { stmt ->
                                            updateStatements.forEach { stmt.addBatch(it) }

                                            stmt.executeBatch()
                                            conn.commit()
                                        }

                                    }
                        }
            }
        }
    }

    /**
     * Revokes all privileges for a user on an organization's database
     * when that user is removed from an organization.
     */
    fun revokeAllPrivilegesFromMember(orgId: UUID, userId: String) {
        val dbName = externalDbManager.getOrganizationDatabaseName(orgId)
        val userName = getDBUser(userId)
        externalDbManager.connect(dbName).connection.use { conn ->
            val stmt = conn.createStatement()
            stmt.execute("REVOKE ALL ON DATABASE $dbName FROM $userName")
        }
    }

    fun syncPermissions(
            orgOwnerAclKeys: List<AclKey>,
            table: OrganizationExternalDatabaseTable,
            maybeColumnId: Optional<UUID>,
            maybeColumnName: Optional<String>
    ): List<Acl> {
        val privilegesByUser = mutableMapOf<AclKey, MutableSet<PostgresPrivileges>>()

        val aclKey = if (maybeColumnId.isPresent) AclKey(table.id, maybeColumnId.get()) else AclKey(table.id)
        val (sql, objectType) = getPrivilegesFields(table.schema, table.name, maybeColumnName)

        //if column objects, sync postgres privileges
        if (maybeColumnId.isPresent) {

            val usernamesToPrivileges = BasePostgresIterable(
                    StatementHolderSupplier(externalDbManager.connectToOrg(table.organizationId), sql)
            ) { rs ->
                user(rs) to PostgresPrivileges.valueOf(privilegeType(rs).toUpperCase())
            }.toMap()

            val usernamesToAclKeys = dbCredentialService.getSecurablePrincipalAclKeysFromUsernames(usernamesToPrivileges.keys)

            usernamesToPrivileges
                    .forEach { (username, privileges) ->
                        usernamesToAclKeys[username]?.let { aclKey ->
                            privilegesByUser.getOrPut(aclKey) { mutableSetOf() }.add(privileges)
                        }
                    }
        }

        //give organization owners all privileges
        orgOwnerAclKeys.forEach { orgOwnerAclKey ->
            privilegesByUser.getOrPut(orgOwnerAclKey) { mutableSetOf() }.addAll(OWNER_PRIVILEGES)
        }

        return privilegesByUser.map { (securablePrincipalAclKey, privileges) ->
            val principal = securePrincipalsManager.getSecurablePrincipal(securablePrincipalAclKey).principal
            val aceKey = AceKey(aclKey, principal)
            val permissions = getPermissionsFromPrivileges(privileges)

            aces.executeOnKey(aceKey, PermissionMerger(permissions, objectType, OffsetDateTime.MAX))
            return@map Acl(aclKey, setOf(Ace(principal, permissions, Optional.empty())))
        }
    }

    /*PRIVATE FUNCTIONS*/
    private fun getNameFromFqnString(fqnString: String): String {
        return FullQualifiedName(fqnString).name
    }

    private fun createDropColumnSql(columnNames: Set<String>): String {
        return columnNames.joinToString(", ") { "DROP COLUMN ${quote(it)}" }
    }

    private fun getUdpateStatementsForAces(
            action: Action,
            table: OrganizationExternalDatabaseTable,
            columnName: String,
            aces: Iterable<Ace>,
            usernamesByPrincipal: Map<Principal, String>
    ): List<String> {
        val statements = mutableListOf<String>()

        aces
                .mapNotNull { ace -> usernamesByPrincipal[ace.principal]?.let { it to ace.permissions } }
                .forEach { (username, permissions) ->

                    //revoke any previous privileges before setting specified ones
                    if (action == Action.SET) {
                        statements.add(
                                createPrivilegesUpdateSql(
                                        Action.REMOVE,
                                        "ALL",
                                        table,
                                        columnName,
                                        username
                                )
                        )
                    }

                    statements.add(
                            createPrivilegesUpdateSql(
                                    action,
                                    getPrivilegesFromPermissions(permissions).joinToString(),
                                    table,
                                    columnName,
                                    username
                            )
                    )
                }

        return statements
    }

    private fun createPrivilegesUpdateSql(action: Action, privileges: String, table: OrganizationExternalDatabaseTable, columnName: String, dbUser: String): String {
        checkState(action == Action.REMOVE || action == Action.ADD || action == Action.SET,
                "Invalid action $action specified")

        val tableInSchema = "${table.schema}.${quote(table.name)}"

        val (grantType, toOrFrom) = if (action == Action.REMOVE) {
            "REVOKE" to "FROM"
        } else {
            "GRANT" to "TO"
        }

        return "$grantType $privileges (${quote(columnName)}) ON $tableInSchema $toOrFrom $dbUser"
    }

    private fun getDbUsers(principals: Collection<Principal>): Map<Principal, String> {

        val securePrincipals = securePrincipalsManager.getSecurablePrincipals(principals).associateBy { it.aclKey }

        return dbCredentialService.getDbAccounts(securePrincipals.values.map { it.aclKey }.toSet()).entries.associate {
            securePrincipals.getValue(it.key).principal to it.value.username
        }
    }

    private fun getDBUser(principalId: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principalId)
        checkState(securePrincipal.principalType == PrincipalType.USER, "Principal must be of type USER")
        return dbCredentialService.getDbUsername(securePrincipal)
    }

    private fun ensurePermissionsAreValid(acls: List<Acl>) {
        acls.forEach {
            it.aces.map { ace -> ace.permissions }.forEach { permissions ->
                if (!permissions.contains(Permission.OWNER) && !permissions.contains(Permission.READ) && !permissions.contains(Permission.WRITE)) {
                    throw IllegalStateException("Permissions $permissions (on AclKey ${it.aclKey}) are not valid -- must contain at least one of OWNER, READ, or WRITE.")
                }
            }
        }

    }

    private fun getPrivilegesFromPermissions(permissions: Set<Permission>): List<String> {
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

    private fun getPermissionsFromPrivileges(privileges: Set<PostgresPrivileges>): EnumSet<Permission> {
        if (OWNER_PRIVILEGES.containsAll(privileges)) {
            return EnumSet.of(Permission.OWNER, Permission.READ, Permission.WRITE)
        }

        val permissions = EnumSet.noneOf(Permission::class.java)

        if (privileges.contains(PostgresPrivileges.SELECT)) {
            permissions.add(Permission.READ)
        }
        if (privileges.contains(PostgresPrivileges.INSERT) || privileges.contains(PostgresPrivileges.UPDATE))
            permissions.add(Permission.WRITE)

        return permissions
    }

    private fun getPrivilegesFields(tableSchema: String, tableName: String, maybeColumnName: Optional<String>): Pair<String, SecurableObjectType> {
        var columnCondition = ""
        var grantsTableName = "information_schema.role_table_grants"
        var objectType = SecurableObjectType.OrganizationExternalDatabaseTable
        if (maybeColumnName.isPresent) {
            val columnName = maybeColumnName.get()
            columnCondition = "AND column_name = '$columnName'"
            grantsTableName = "information_schema.role_column_grants"
            objectType = SecurableObjectType.OrganizationExternalDatabaseColumn
        }
        val sql = getCurrentUsersPrivilegesSql(tableSchema, tableName, grantsTableName, columnCondition)
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
            externalDbManager.connect(dbName).connection.use { conn ->
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

    /**
     * Moves a table from the [STAGING_SCHEMA] schema to the [OPENLATTICE_SCHEMA] schema
     */
    fun promoteStagingTable(organizationId: UUID, tableName: String) {
        val singletonTableSet = organizationExternalDatabaseTables.keySet(
                Predicates.and(
                        Predicates.equal<UUID, OrganizationExternalDatabaseTable>(ORGANIZATION_ID_INDEX, organizationId),
                        Predicates.equal<UUID, OrganizationExternalDatabaseTable>(NAME_INDEX, tableName),
                        Predicates.equal<UUID, OrganizationExternalDatabaseTable>(SCHEMA_INDEX, STAGING_SCHEMA)
                )
        )

        if (singletonTableSet.isEmpty()) {
            throw IllegalArgumentException("Cannot promote table -- there is no table named $tableName in the staging schema of org $organizationId")
        }

        externalDbManager.connectToOrg(organizationId).use { hds ->
            hds.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(publishStagingTableSql(tableName))
                }
            }
        }

        organizationExternalDatabaseTables.executeOnKey(singletonTableSet.first()) {
            val table = it.value
            table.schema = OPENLATTICE_SCHEMA
            it.setValue(table)
        }
    }

    /*INTERNAL SQL QUERIES*/

    /**
     * For a database, retrieves a ResultSet enumerating all tables and columns in the openlattice and staging schemas.
     * Each row maps to a single table, containing the following columns
     * - oid: the table's OID
     * - name: table name
     * - schema_name: the table's schema
     * - column_names: array of the table's columnds
     */
    private fun getCurrentTableAndColumnNamesSql(): String {
        return """
            SELECT
              $oidFromPgTables,
              information_schema.tables.table_name AS ${NAME.name},
              information_schema.tables.table_schema AS $SCHEMA_NAME_FIELD,
              (
                SELECT '{}' || array_agg(col_name::text)
                FROM UNNEST(array_agg(information_schema.columns.column_name)) col_name
                WHERE col_name IS NOT NULL
              ) AS $COLUMN_NAMES_FIELD
            $fromExpression $leftJoinColumnsExpression
            WHERE
              information_schema.tables.table_schema=ANY('{$OPENLATTICE_SCHEMA,$STAGING_SCHEMA}')
              AND table_type='BASE TABLE'
            GROUP BY name, information_schema.tables.table_schema;
        """.trimIndent()
    }

    private val oidFromPgTables = "(information_schema.tables.table_schema || '.' || quote_ident(information_schema.tables.table_name))::regclass::oid AS ${OID.name}"

    private fun getColumnMetadataSql(tableName: String, columnCondition: String): String {
        return selectExpression + ", information_schema.columns.data_type AS datatype, " +
                "information_schema.columns.ordinal_position, " +
                "information_schema.table_constraints.constraint_type " +
                fromExpression + leftJoinColumnsExpression + leftJoinPgClass +
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

    private fun getCurrentUsersPrivilegesSql(tableSchema: String, tableName: String, grantsTableName: String, columnCondition: String): String {
        return "SELECT grantee AS user, privilege_type " +
                "FROM $grantsTableName " +
                "WHERE table_name = '$tableName' " +
                "AND table_schema = '$tableSchema' " +
                columnCondition
    }

    private fun getReloadConfigSql(): String {
        return "SELECT pg_reload_conf()"
    }

    private val selectExpression = "SELECT oid, information_schema.tables.table_name AS name, information_schema.columns.column_name "

    private val fromExpression = "FROM information_schema.tables "

    private val leftJoinPgClass = " LEFT JOIN pg_class ON relname = information_schema.tables.table_name "
    private val leftJoinColumnsExpression = """
        LEFT JOIN information_schema.columns
          ON information_schema.tables.table_name = information_schema.columns.table_name
          AND information_schema.tables.table_schema = information_schema.columns.table_schema
    """.trimIndent()

    private fun getInsertRecordSql(table: PostgresTableDefinition, columns: String, pkey: String, record: PostgresAuthenticationRecord): String {
        return "INSERT INTO ${table.name} $columns VALUES(${record.buildPostgresRecord()}) " +
                "ON CONFLICT $pkey DO UPDATE SET ${PostgresColumn.AUTHENTICATION_METHOD.name}=EXCLUDED.${PostgresColumn.AUTHENTICATION_METHOD.name}"
    }

    private fun getDeleteRecordSql(table: PostgresTableDefinition, record: PostgresAuthenticationRecord): String {
        return "DELETE FROM ${table.name} WHERE ${PostgresColumn.USERNAME.name} = '${record.username}' " +
                "AND ${DATABASE.name} = '${record.database}' " +
                "AND ${CONNECTION_TYPE.name} = '${record.connectionType}' " +
                "AND ${IP_ADDRESS.name} = '${record.ipAddress}'"
    }

    private fun getSelectRecordsSql(tableName: String): String {
        return "SELECT * FROM $tableName"
    }

    /*PREDICATES*/
    private fun <T> idsPredicate(ids: Set<UUID>): Predicate<UUID, T> {
        return Predicates.`in`(QueryConstants.KEY_ATTRIBUTE_NAME.value(), *ids.toTypedArray())
    }

    private fun belongsToOrganization(orgId: UUID): Predicate<UUID, OrganizationExternalDatabaseTable> {
        return Predicates.equal(ORGANIZATION_ID_INDEX, orgId)
    }

    private fun belongsToTable(tableId: UUID): Predicate<UUID, OrganizationExternalDatabaseColumn> {
        return Predicates.equal(TABLE_ID_INDEX, tableId)
    }

    private fun belongsToTables(tableIds: Collection<UUID>): Predicate<UUID, OrganizationExternalDatabaseColumn> {
        return Predicates.`in`(TABLE_ID_INDEX, *tableIds.toTypedArray())
    }

    private fun publishStagingTableSql(tableName: String): String {
        return "ALTER TABLE ${quote(tableName)} SET SCHEMA $OPENLATTICE_SCHEMA"
    }

}

data class TableInfo(
        val oid: Int,
        val tableName: String,
        val schemaName: String,
        val columnNames: List<String>
)
