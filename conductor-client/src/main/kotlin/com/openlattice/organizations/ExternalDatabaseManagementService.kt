package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PermissionMapstore
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.datasets.DataSetService
import com.openlattice.datasets.SecurableObjectMetadata
import com.openlattice.datasets.SecurableObjectMetadataUpdate
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.edm.processors.AddFlagsOnEntitySetEntryProcessor
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetFqnFromPropertyTypeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.organizations.ExternalTableEntryProcessor
import com.openlattice.hazelcast.processors.organizations.UpdateExternalColumnEntryProcessor
import com.openlattice.hazelcast.processors.organizations.UpdateExternalTableEntryProcessor
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organization.ExternalTableColumnsPair
import com.openlattice.organizations.mapstores.ExternalTablesMapstore.Companion.NAME_INDEX
import com.openlattice.organizations.mapstores.ExternalTablesMapstore.Companion.SCHEMA_INDEX
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.organizations.mapstores.TABLE_ID_INDEX
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.COLUMN_NAMES_FIELD
import com.openlattice.postgres.PostgresColumn.CONNECTION_TYPE
import com.openlattice.postgres.PostgresColumn.DATABASE
import com.openlattice.postgres.PostgresColumn.IP_ADDRESS
import com.openlattice.postgres.PostgresColumn.NAME
import com.openlattice.postgres.PostgresColumn.OID
import com.openlattice.postgres.PostgresColumn.SCHEMA_NAME_FIELD
import com.openlattice.postgres.ResultSetAdapters.columnName
import com.openlattice.postgres.ResultSetAdapters.columnNames
import com.openlattice.postgres.ResultSetAdapters.constraintType
import com.openlattice.postgres.ResultSetAdapters.name
import com.openlattice.postgres.ResultSetAdapters.oid
import com.openlattice.postgres.ResultSetAdapters.ordinalPosition
import com.openlattice.postgres.ResultSetAdapters.postgresAuthenticationRecord
import com.openlattice.postgres.ResultSetAdapters.privilegeType
import com.openlattice.postgres.ResultSetAdapters.schemaName
import com.openlattice.postgres.ResultSetAdapters.sqlDataType
import com.openlattice.postgres.ResultSetAdapters.user
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.external.Schemas.INTEGRATIONS_SCHEMA
import com.openlattice.postgres.external.Schemas.OPENLATTICE_SCHEMA
import com.openlattice.postgres.external.Schemas.STAGING_SCHEMA
import com.openlattice.postgres.external.dropAllConnectionsToDatabaseSql
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.openlattice.transporter.processors.GetPropertyTypesFromTransporterColumnSetEntryProcessor
import com.openlattice.transporter.services.TransporterService
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
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

@Service
class ExternalDatabaseManagementService(
    hazelcastInstance: HazelcastInstance,
    private val externalDbManager: ExternalDatabaseConnectionManager,
    private val principalsMapManager: PrincipalsMapManager,
    private val aclKeyReservations: HazelcastAclKeyReservationService,
    private val authorizationManager: AuthorizationManager,
    private val organizationExternalDatabaseConfiguration: OrganizationExternalDatabaseConfiguration,
    private val extDbPermsManager: ExternalDatabasePermissioningService,
    private val transporterService: TransporterService,
    private val dbCredentialService: DbCredentialService,
    private val hds: HikariDataSource,
    private val dataSetService: DataSetService
) {

    private val logger = LoggerFactory.getLogger(ExternalDatabaseManagementService::class.java)

    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(hazelcastInstance)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(hazelcastInstance)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)

    private val primaryKeyConstraint = "PRIMARY KEY"
    private val FETCH_SIZE = 100_000

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val transporterState = HazelcastMap.TRANSPORTER_DB_COLUMNS.getMap(hazelcastInstance)
    private val permissions = HazelcastMap.PERMISSIONS.getMap(hazelcastInstance)

    companion object {
        private val OWNER_PRIVILEGES = PostgresPrivileges.values().toSet() - PostgresPrivileges.ALL
    }

    /*CREATE*/
    fun createOrganizationExternalDatabaseTable(orgId: UUID, table: ExternalTable): UUID {
        val tableUniqueName = table.getUniqueName()
        checkState(externalTables.putIfAbsent(table.id, table) == null,
                "ExternalTable $tableUniqueName already exists")
        aclKeyReservations.reserveIdAndValidateType(table) { tableUniqueName }

        val tableAclKey = AclKey(table.id)
        authorizationManager.setSecurableObjectType(tableAclKey, SecurableObjectType.OrganizationExternalDatabaseTable)

        dataSetService.initializeMetadata(tableAclKey, SecurableObjectMetadata.fromExternalTable(table))
        dataSetService.indexDataSet(table.id)

        return table.id
    }

    fun createOrganizationExternalDatabaseColumn(orgId: UUID, column: ExternalColumn): UUID {
        checkState(externalTables[column.tableId] != null,
                "ExternalColumn ${column.name} belongs to " +
                        "a table with id ${column.tableId} that does not exist")
        val columnUniqueName = column.getUniqueName()
        checkState(externalColumns.putIfAbsent(column.id, column) == null,
                "ExternalColumn $columnUniqueName already exists")
        aclKeyReservations.reserveIdAndValidateType(column) { columnUniqueName }

        authorizationManager.setSecurableObjectType(column.getAclKey(), SecurableObjectType.OrganizationExternalDatabaseColumn)

        dataSetService.initializeMetadata(column.getAclKey(), SecurableObjectMetadata.fromExternalColumn(column))

        return column.id
    }

    fun getColumnMetadata(
            table: ExternalTable
    ): List<ExternalColumn> {

        val sql = getColumnMetadataSql(table.schema, table.name)

        return BasePostgresIterable(
                StatementHolderSupplier(externalDbManager.connectToOrg(table.organizationId), sql)
        ) { rs ->
            try {
                val storedColumnName = columnName(rs)
                val dataType = sqlDataType(rs)
                val position = ordinalPosition(rs)
                val isPrimaryKey = constraintType(rs) == primaryKeyConstraint
                ExternalColumn(
                        Optional.empty(),
                        storedColumnName,
                        storedColumnName,
                        Optional.empty(),
                        table.id,
                        table.organizationId,
                        dataType,
                        isPrimaryKey,
                        position)
            } catch (e: Exception) {
                logger.error("Unable to map column to ExternalColumn object for table {}.{} in organization {}", table.schema, table.name, table.organizationId, e)
                null
            }
        }.filterNotNull()
    }

    fun destroyTransportedEntitySet(organizationId: UUID, entitySetId: UUID) {
        try {
            entitySets.lock(entitySetId, 10, TimeUnit.SECONDS)
            val es = entitySets.getValue(entitySetId)

            transporterService.disassembleEntitySet(organizationId, es.id, es.entityTypeId, es.name)

            es.flags.remove(EntitySetFlag.TRANSPORTED)
            entitySets.set(entitySetId, es)
        } finally {
            entitySets.unlock(entitySetId)
        }
    }

    fun transportEntitySet(organizationId: UUID, entitySetId: UUID): CompletableFuture<Boolean> {
        val ptIds = entitySets.submitToKey(
                entitySetId,
                GetEntityTypeFromEntitySetEntryProcessor()
        ).thenCompose { etid ->
            requireNotNull(etid) {
                "Entity set $entitySetId has no entity type"
            }
            transporterState.submitToKey(etid, GetPropertyTypesFromTransporterColumnSetEntryProcessor())
        }

        val ptIdsToFqns = ptIds.thenCompose { transporterPtIds ->
            propertyTypes.submitToKeys(transporterPtIds, GetFqnFromPropertyTypeEntryProcessor())
        }.thenApply { ptIdToFqn ->
            ptIdToFqn.mapTo(mutableSetOf()) { PropertyTypeIdFqn(it.key, it.value) }
        }

        val tableCols = ptIds.thenApply { transporterPtIds ->
            transporterPtIds.associate {
                AclKey(entitySetId, it) to TableColumn(organizationId, entitySetId, it)
            }
        }

        val acls = permissions.entrySet(Predicates.and(
                Predicates.equal<AceKey, AceValue>(PermissionMapstore.ROOT_OBJECT_INDEX, entitySetId),
                Predicates.equal<AceKey, AceValue>(PermissionMapstore.SECURABLE_OBJECT_TYPE_INDEX, SecurableObjectType.PropertyTypeInEntitySet),
                Predicates.`in`<AceKey, AceValue>(PermissionMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.USER, PrincipalType.ROLE, PrincipalType.ORGANIZATION))
        ).groupBy({ (aceKey, _) ->
            aceKey.aclKey
        }, { (aceKey, aceValue) ->
            Ace(aceKey.principal, aceValue as Set<Permission>, Optional.of(aceValue.expirationDate))
        }).map {
            Acl(it.key, it.value)
        }

        return ptIdsToFqns.thenCombineAsync(tableCols) { asPtFqns, colsById ->
            try {
                entitySets.lock(entitySetId, 10, TimeUnit.SECONDS)
                val es = entitySets.getValue(entitySetId)
                transporterService.assembleEntitySet(
                        organizationId,
                        es,
                        asPtFqns,
                        acls,
                        colsById
                )
                entitySets.executeOnKey(entitySetId, AddFlagsOnEntitySetEntryProcessor(EnumSet.of(EntitySetFlag.TRANSPORTED)))
            } catch (ex: Exception) {
                logger.error("error while transporting entityset $entitySetId: ", ex)
                return@thenCombineAsync false
            } finally {
                entitySets.unlock(entitySetId)
            }
            return@thenCombineAsync true
        }.toCompletableFuture()
    }

    /*GET*/

    fun getExternalDatabaseTables(orgId: UUID): Map<UUID, ExternalTable> {
        return externalTables.values(belongsToOrganization(orgId)).associateBy { it.id }
    }

    /**
     * Returns a map from tableId to its columns, as a map from columnId to column
     */
    fun getColumnsForTables(tableIds: Set<UUID>): Map<UUID, Map<UUID, ExternalColumn>> {
        return externalColumns
                .values(belongsToTables(tableIds))
                .groupBy { it.tableId }
                .mapValues { it.value.associateBy { c -> c.id } }
    }

    fun getExternalDatabaseTableWithColumns(tableId: UUID): ExternalTableColumnsPair {
        val table = getExternalTable(tableId)
        return ExternalTableColumnsPair(table, externalColumns.values(belongsToTable(tableId)).toSet())
    }

    fun getExternalDatabaseTableData(
            orgId: UUID,
            tableId: UUID,
            authorizedColumns: Set<ExternalColumn>,
            rowCount: Int): Map<UUID, List<Any?>> {
        val tableName = externalTables.getValue(tableId).name
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

    fun getExternalTable(tableId: UUID): ExternalTable {
        return externalTables.getValue(tableId)
    }

    fun getExternalColumn(columnId: UUID): ExternalColumn {
        return externalColumns.getValue(columnId)
    }

    fun getTableInfoForOrganization(organizationId: UUID): List<TableInfo> {
        return BasePostgresIterable(StatementHolderSupplier(
                externalDbManager.connectToOrg(organizationId),
                getCurrentTableAndColumnNamesSql(),
                FETCH_SIZE
        )) { rs ->
            TableInfo(oid(rs), name(rs), schemaName(rs), columnNames(rs))
        }.toList()
    }

    fun executePrivilegesUpdate(action: Action, acls: List<Acl>) {
        extDbPermsManager.executePrivilegesUpdate(action, acls)
    }

    /*UPDATE*/
    fun updateExternalTable(orgId: UUID, tableFqnToId: Pair<String, UUID>, update: MetadataUpdate) {
        update.name.ifPresent {
            val newTableFqn = FullQualifiedName(orgId.toString(), it)
            val oldTableName = getNameFromFqnString(tableFqnToId.first)
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ALTER TABLE $oldTableName RENAME TO $it")
                }
            }
            aclKeyReservations.renameReservation(tableFqnToId.second, newTableFqn.fullQualifiedNameAsString)
        }

        updateExternalTableMetadata(tableFqnToId.second, update)
        dataSetService.updateObjectMetadata(AclKey(tableFqnToId.second), SecurableObjectMetadataUpdate.fromMetadataUpdate(update))
    }

    fun updateExternalTableMetadata(tableId: UUID, update: MetadataUpdate) {
        externalTables.submitToKey(tableId, UpdateExternalTableEntryProcessor(update))
    }

    fun updateExternalColumn(orgId: UUID, tableFqnToId: Pair<String, UUID>, columnFqnToId: Pair<String, UUID>, update: MetadataUpdate) {
        update.name.ifPresent {
            val tableName = getNameFromFqnString(tableFqnToId.first)
            val newColumnFqn = FullQualifiedName(tableFqnToId.second.toString(), it)
            val oldColumnName = getNameFromFqnString(columnFqnToId.first)
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ALTER TABLE $tableName RENAME COLUMN $oldColumnName to $it")
                }
            }
            aclKeyReservations.renameReservation(columnFqnToId.second, newColumnFqn.fullQualifiedNameAsString)
        }
        updateExternalColumnMetadata(tableFqnToId.second, columnFqnToId.second, update)
        dataSetService.updateObjectMetadata(
                AclKey(tableFqnToId.second, columnFqnToId.second),
                SecurableObjectMetadataUpdate.fromMetadataUpdate(update)
        )
    }

    fun updateExternalColumnMetadata(tableId: UUID, columnId: UUID, update: MetadataUpdate) {
        externalColumns.submitToKey(columnId, UpdateExternalColumnEntryProcessor(update))
    }

    /*DELETE*/
    fun deleteExternalTables(orgId: UUID, tableIdByFqn: Map<String, UUID>) {
        tableIdByFqn.forEach { (tableFqn, tableId) ->
            val tableName = getNameFromFqnString(tableFqn)

            //delete columns from tables
            val tableFqnToId = Pair(tableFqn, tableId)
            val columnIdByFqn = externalColumns
                    .entrySet(belongsToTable(tableId))
                    .map { FullQualifiedName(tableId.toString(), it.value.name).toString() to it.key }
                    .toMap()
            val columnsByTable = mapOf(tableFqnToId to columnIdByFqn)
            deleteExternalColumns(orgId, columnsByTable)

            //delete tables from postgres
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE $tableName")
                }
            }
        }

        //delete securable object
        val tableIds = tableIdByFqn.values.toSet()
        deleteExternalTableObjects(tableIds)
    }

    fun deleteExternalTableObjects(tableIds: Set<UUID>) {
        tableIds.forEach {
            val aclKey = AclKey(it)
            dataSetService.deleteObjectMetadata(aclKey)
            authorizationManager.deletePermissions(aclKey)
            securableObjectTypes.remove(aclKey)
            aclKeyReservations.release(it)
        }
        externalTables.removeAll(idsPredicate(tableIds))
    }

    fun deleteExternalColumns(orgId: UUID, columnsByTable: Map<Pair<String, UUID>, Map<String, UUID>>) {
        columnsByTable.forEach { (tableFqnToId, columnIdsByFqn) ->
            if (columnIdsByFqn.isEmpty()) return@forEach

            val tableName = getNameFromFqnString(tableFqnToId.first)
            val tableId = tableFqnToId.second
            val columnNames = columnIdsByFqn.keys.map { getNameFromFqnString(it) }.toSet()
            val columnIds = columnIdsByFqn.values.toSet()

            //delete columns from postgres
            val dropColumnsSql = createDropColumnSql(columnNames)
            externalDbManager.connectToOrg(orgId).connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ALTER TABLE $tableName $dropColumnsSql")
                }
            }

            deleteExternalColumnObjects(orgId, mapOf(tableId to columnIds))
        }
    }

    fun deleteExternalColumnObjects(organizationId: UUID, columnIdsByTableId: Map<UUID, Set<UUID>>) {
        columnIdsByTableId.forEach { (tableId, columnIds) ->
            columnIds.forEach { columnId ->
                val aclKey = AclKey(tableId, columnId)
                dataSetService.deleteObjectMetadata(aclKey)
                authorizationManager.deletePermissions(aclKey)
                securableObjectTypes.remove(aclKey)
                aclKeyReservations.release(columnId)
            }
            externalColumns.removeAll(idsPredicate(columnIds))
        }

        extDbPermsManager.destroyExternalTablePermissions(columnIdsByTableId)
    }

    /**
     * Deletes an organization's entire database.
     * Is called when an organization is deleted.
     */
    fun deleteOrganizationExternalDatabase(orgId: UUID) {
        //remove all tables/columns within org
        val tableIdByFqn = externalTables
                .entrySet(belongsToOrganization(orgId))
                .map { FullQualifiedName(orgId.toString(), it.value.name).toString() to it.key }
                .toMap()

        deleteExternalTables(orgId, tableIdByFqn)

        //drop db from schema
        val dbName = externalDbManager.getDatabaseName(orgId)
        externalDbManager.connectAsSuperuser().connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(dropAllConnectionsToDatabaseSql(dbName))
                stmt.execute("DROP DATABASE $dbName")
            }
        }


        externalDbManager.deleteOrganizationDatabase(orgId)
    }

    /*PERMISSIONS*/
    fun addHBARecord(orgId: UUID, userPrincipal: Principal, connectionType: PostgresConnectionType, ipAddress: String) {
        operateOnHBARecord(orgId, userPrincipal, connectionType, ipAddress) { hds, pgAuthRecord ->
            hds.connection.use { connection ->
                connection.createStatement().use { stmt ->
                    val hbaTable = PostgresTable.HBA_AUTHENTICATION_RECORDS
                    val columns = hbaTable.columns.joinToString(", ", "(", ")") { it.name }
                    val pkey = hbaTable.primaryKey.joinToString(", ", "(", ")") { it.name }
                    val insertRecordSql = getInsertRecordSql(hbaTable, columns, pkey, pgAuthRecord)
                    stmt.executeUpdate(insertRecordSql)
                }
            }
        }
    }

    fun removeHBARecord(orgId: UUID, userPrincipal: Principal, connectionType: PostgresConnectionType, ipAddress: String) {
        operateOnHBARecord(orgId, userPrincipal, connectionType, ipAddress) { hds, pgAuthRecord ->
            hds.connection.use {
                it.createStatement().use { stmt ->
                    val hbaTable = PostgresTable.HBA_AUTHENTICATION_RECORDS
                    val deleteRecordSql = getDeleteRecordSql(hbaTable, pgAuthRecord)
                    stmt.executeUpdate(deleteRecordSql)
                }
            }
        }
    }

    fun operateOnHBARecord(
            orgId: UUID,
            userPrincipal: Principal,
            connectionType: PostgresConnectionType,
            ipAddress: String,
            operation: (hds: HikariDataSource, pgAuthRecord: PostgresAuthenticationRecord) -> Unit
    ) {
        val dbName = externalDbManager.getDatabaseName(orgId)
        val username = getDBUser(userPrincipal.id)
        val record = PostgresAuthenticationRecord(
                connectionType,
                dbName,
                username,
                ipAddress,
                organizationExternalDatabaseConfiguration.authMethod)
        operation(hds, record)
        updateHBARecords(orgId)
    }

    /**
     * Revokes all privileges for a user on an organization's database
     * when that user is removed from an organization.
     */
    fun revokeAllPrivilegesFromMember(orgId: UUID, userId: String) {
        val userName = getDBUser(userId)
        val (hds, dbName) = externalDbManager.connectToOrgGettingName(orgId)
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("REVOKE ALL ON DATABASE $dbName FROM $userName")
            }
        }
    }

    fun syncPermissions(
            adminRolePrincipal: Principal,
            table: ExternalTable,
            columns: Set<ExternalColumn>,
            maybeColumnId: UUID? = null,
            maybeColumnName: String? = null
    ): List<Acl> {
        if (columns.isEmpty()) {
            return listOf()
        }

        val columnNameToAclKey = columns.associate { it.name to it.getAclKey() }
        val aclKeysToGrant = mutableSetOf(AclKey(table.id)) + columnNameToAclKey.values

        // Load any existing privileges on columns
        val columnPrivilegesSql = getPrivilegesOnColumnsSql(table.schema, table.name, columns.map { it.name })
        val orgHDS = externalDbManager.connectToOrg(table.organizationId)

        val columnToUserToPrivileges = BasePostgresIterable(StatementHolderSupplier(orgHDS, columnPrivilegesSql)) { rs ->
            try {
                columnName(rs) to (user(rs) to PostgresPrivileges.valueOf(privilegeType(rs).toUpperCase()))
            } catch (e: Exception) {
                logger.error("Unable to sync privilege row for table {}", table.id, e)
                null
            }
        }
                .filterNotNull()
                .groupBy { columnNameToAclKey.getValue(it.first) }
                .mapValues {
                    it.value.map { pair -> pair.second }
                            .groupBy { (username, _) -> username }
                            .mapValues { entry -> entry.value.map { pair -> pair.second }.toSet() }
                }


        // Map username -> Principal
        val usernamesToAclKeys = dbCredentialService.getSecurablePrincipalAclKeysFromUsernames(columnToUserToPrivileges.flatMap {
            it.value.keys
        })
        val aclKeyToPrincipal = principalsMapManager.getSecurablePrincipals(usernamesToAclKeys.values.toSet())
        val usernameToPrincipal = usernamesToAclKeys.mapValues { aclKeyToPrincipal[it.value]?.principal }


        // Compute set of Acl grants based on existing postgres grants + ownership for admin role, and perform grants
        val adminRoleAce = Ace(adminRolePrincipal, EnumSet.allOf(Permission::class.java))
        val aclGrants = mutableListOf<Acl>()
        aclKeysToGrant.forEach { objAclKey ->
            val aces = (columnToUserToPrivileges[objAclKey] ?: mutableMapOf()).mapNotNull { (username, privileges) ->
                val principal = usernameToPrincipal[username] ?: return@mapNotNull null
                Ace(principal, getPermissionsFromPrivileges(privileges))
            } + adminRoleAce

            aclGrants.add(Acl(objAclKey, aces))
        }

        val tableAces = columnToUserToPrivileges.values.flatMap { it.entries }.groupBy { it.key }.mapNotNull { (username, privilegesMap) ->
            if (privilegesMap.isEmpty()) {
                return@mapNotNull null
            }
            val principal = usernameToPrincipal[username] ?: return@mapNotNull null

            val intersection = privilegesMap.first().value.toMutableSet()
            privilegesMap.map { it.value }.forEach { privileges ->
                intersection.removeIf { !privileges.contains(it) }
            }

            Ace(principal, getPermissionsFromPrivileges(intersection))
        }
        if (tableAces.isNotEmpty()) {
            aclGrants.add(Acl(AclKey(table.id), tableAces))
        }

        authorizationManager.addPermissions(aclGrants)

        return aclGrants
    }

    fun syncAtlasPermissions(
        orgId: UUID,
        columns: Set<ExternalColumn>
    ) {
        val columnAclKeys = columns.map {
            it.getAclKey()
        }
        val acls = permissions.entrySet(
            Predicates.`in`(
                PermissionMapstore.ACL_KEY_INDEX,
                *columnAclKeys.toTypedArray()
            )
        ).groupBy({ it.key.aclKey }, { (aceKey, aceVal) ->
            Ace(aceKey.principal, aceVal.permissions, aceVal.expirationDate)
        })
        .map { (aclKey, aces) -> Acl(aclKey, aces) }

        extDbPermsManager.executePrivilegesUpdate(Action.SET, acls)
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
            table: ExternalTable,
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

    private fun createPrivilegesUpdateSql(action: Action, privileges: String, table: ExternalTable, columnName: String, dbUser: String): String {
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

    private fun getDBUser(principalId: String): String {
        val securePrincipal = principalsMapManager.getSecurablePrincipal(principalId)
        checkState(securePrincipal.principalType == PrincipalType.USER, "Principal must be of type USER")
        return dbCredentialService.getDbUsername(securePrincipal)
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

    private fun updateHBARecords(organizationId: UUID) {
        val originalHBAPath = Paths.get(organizationExternalDatabaseConfiguration.path + organizationExternalDatabaseConfiguration.fileName)
        val tempHBAPath = Paths.get(organizationExternalDatabaseConfiguration.path + "/temp_hba.conf")

        //create hba file with new records
        val records = getHBARecords(PostgresTable.HBA_AUTHENTICATION_RECORDS.name).map {
            it.buildHBAConfRecord()
        }.toSet()

        try {
            Files.newOutputStream(
                    tempHBAPath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            ).use { `in` ->
                BufferedOutputStream(`in`).use { out ->
                    records.forEach {
                        val recordAsByteArray = it.toByteArray()
                        out.write(recordAsByteArray)
                    }
                }
            }
        } catch (ex: IOException) {
            logger.info("IO exception while creating new hba config")
        }

        //reload config
        externalDbManager.connectToOrg(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(getReloadConfigSql())
            }
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
        val singletonTableSet = externalTables.keySet(
                Predicates.and(
                        Predicates.equal<UUID, ExternalTable>(ORGANIZATION_ID_INDEX, organizationId),
                        Predicates.equal<UUID, ExternalTable>(NAME_INDEX, tableName),
                        Predicates.equal<UUID, ExternalTable>(SCHEMA_INDEX, STAGING_SCHEMA)
                )
        )

        if (singletonTableSet.isEmpty()) {
            throw IllegalArgumentException("Cannot promote table -- there is no table named $tableName in the staging schema of org $organizationId")
        }

        externalDbManager.connectToOrg(organizationId).connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(publishStagingTableSql(tableName))
            }
        }

        val table = singletonTableSet.first()
        externalTables.executeOnKey(table, ExternalTableEntryProcessor {
            it.schema = OPENLATTICE_SCHEMA.label
            ExternalTableEntryProcessor.Result()
        })
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
              information_schema.tables.table_schema=ANY('{$OPENLATTICE_SCHEMA,$STAGING_SCHEMA,$INTEGRATIONS_SCHEMA}')
              AND table_type='BASE TABLE'
            GROUP BY name, information_schema.tables.table_schema;
        """.trimIndent()
    }

    private val oidFromPgTables = "(information_schema.tables.table_schema || '.' || quote_ident(information_schema.tables.table_name))::regclass::oid AS ${OID.name}"

    private fun getColumnMetadataSql(tableSchema: String, tableName: String): String {
        return """
            $selectExpression
            $fromExpression
            $leftJoinColumnsExpression
            LEFT OUTER JOIN information_schema.constraint_column_usage
              ON information_schema.columns.column_name = information_schema.constraint_column_usage.column_name
              AND information_schema.columns.table_name = information_schema.constraint_column_usage.table_name
              AND information_schema.columns.table_schema = information_schema.constraint_column_usage.table_schema
            LEFT OUTER JOIN information_schema.table_constraints
              ON information_schema.constraint_column_usage.constraint_name = information_schema.table_constraints.constraint_name
              WHERE information_schema.columns.table_name = '$tableName'
              AND information_schema.columns.table_schema = '$tableSchema'
              AND (
                information_schema.table_constraints.constraint_type = 'PRIMARY KEY'
                OR information_schema.table_constraints.constraint_type IS NULL
              )
        """.trimIndent()
    }

    private fun getPrivilegesOnColumnsSql(tableSchema: String, tableName: String, columnNames: List<String>): String {
        val colNamesSql = columnNames.joinToString { quote(it) }

        return """
            SELECT grantee AS user, privilege_type, column_name
            FROM information_schema.role_column_grants
            WHERE table_name = '$tableName' 
            AND table_schema = '$tableSchema'
            AND column_name = ANY('{$colNamesSql}')
        """.trimIndent()
    }

    private fun getReloadConfigSql(): String {
        return "SELECT pg_reload_conf()"
    }

    private val selectExpression = """
        SELECT
          $oidFromPgTables,
          information_schema.tables.table_name AS name,
          information_schema.columns.column_name,
          information_schema.columns.data_type AS datatype,
          information_schema.columns.ordinal_position,
          information_schema.table_constraints.constraint_type
    """.trimIndent()

    private val fromExpression = "FROM information_schema.tables "

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

    private fun belongsToOrganization(orgId: UUID): Predicate<UUID, ExternalTable> {
        return Predicates.equal(ORGANIZATION_ID_INDEX, orgId)
    }

    private fun belongsToTable(tableId: UUID): Predicate<UUID, ExternalColumn> {
        return Predicates.equal(TABLE_ID_INDEX, tableId)
    }

    private fun belongsToTables(tableIds: Collection<UUID>): Predicate<UUID, ExternalColumn> {
        return Predicates.`in`(TABLE_ID_INDEX, *tableIds.toTypedArray())
    }

    private fun publishStagingTableSql(tableName: String): String {
        return "ALTER TABLE $STAGING_SCHEMA.${quote(tableName)} SET SCHEMA $OPENLATTICE_SCHEMA"
    }

}

data class TableInfo(
    val oid: Long,
    val tableName: String,
    val schemaName: String,
    val columnNames: List<String>
)
