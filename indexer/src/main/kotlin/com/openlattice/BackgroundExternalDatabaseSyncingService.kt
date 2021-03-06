package com.openlattice

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMap
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.auditing.AuditEventType
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditableEvent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.*
import com.openlattice.datasets.DataSetService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastMap.Companion.EXTERNAL_COLUMNS
import com.openlattice.hazelcast.HazelcastMap.Companion.EXTERNAL_TABLES
import com.openlattice.hazelcast.HazelcastMap.Companion.ORGANIZATION_DATABASES
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.postgres.TableColumn
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.external.Schemas
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class BackgroundExternalDatabaseSyncingService(
    hazelcastInstance: HazelcastInstance,
    private val edms: ExternalDatabaseManagementService,
    private val extDbPermsService: ExternalDatabasePermissioningService,
    private val auditingManager: AuditingManager,
    private val ares: AuditRecordEntitySetsManager,
    private val indexerConfiguration: IndexerConfiguration,
    private val organizationMetadataEntitySetsService: OrganizationMetadataEntitySetsService,
    private val reservationService: HazelcastAclKeyReservationService,
    private val principalsMapManager: PrincipalsMapManager,
    private val dataSetService: DataSetService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseSyncingService::class.java)

        const val SCAN_RATE = 1_000L * 30 // 30 seconds
    }

    private val organizationExternalDatabaseColumns = EXTERNAL_COLUMNS.getMap(hazelcastInstance)
    private val organizationExternalDatabaseTables = EXTERNAL_TABLES.getMap(hazelcastInstance)
    private val organizationDatabases = ORGANIZATION_DATABASES.getMap(hazelcastInstance)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)

    private val taskLock = ReentrantLock()

    @Suppress("UNUSED")
    @Scheduled(fixedDelay = SCAN_RATE)
    fun scanOrganizationDatabases() {
        logger.info("Starting background external database sync task.")

        if (!indexerConfiguration.backgroundExternalDatabaseSyncingEnabled) {
            logger.info("Skipping external database syncing as it is not enabled.")
            return
        }

        if (!taskLock.tryLock()) {
            logger.info("Not starting new external database sync task as an existing one is running")
            return
        }

        try {
            val timer = Stopwatch.createStarted()

            organizations.keys
                    .filter { it != IdConstants.GLOBAL_ORGANIZATION_ID.id }
                    .shuffled()
                    .forEach {
                        try {
                            syncOrganizationDatabases(it)
                        } catch (e: Exception) {
                            logger.error("An error occurred when trying to sync database for org {}", it, e)
                        }
                    }

            logger.info("Completed syncing database objects in {}", timer)
        } catch (ex: Exception) {
            logger.error("Failed while syncing external database metadata", ex)
        } finally {
            taskLock.unlock()
        }
    }

    private fun syncOrganizationDatabases(orgId: UUID) {
        val sw = Stopwatch.createStarted()
        logger.info("About to sync database for organization {}", orgId)
        val dbName = organizationDatabases[orgId]?.name

        if (dbName == null) {
            logger.error("Organization {} does not exist in the organizationDatabases mapstore", orgId)
            return
        }

        val adminRoleAclKey = organizations.getValue(orgId).adminRoleAclKey
        val adminRolePrincipal = principalsMapManager.getSecurablePrincipal(adminRoleAclKey)!!.principal

        val tableIds = mutableSetOf<UUID>()
        val columnIds = mutableSetOf<UUID>()

        edms.getTableInfoForOrganization(orgId).forEach { (oid, tableName, schemaName, _) ->
            val table = getOrCreateTable(orgId, oid, tableName, schemaName)
            val columns = syncTableColumns(table)
            dataSetService.indexDataSet(table.id)

            initializeTablePermissions(orgId, table, columns, adminRolePrincipal)

            tableIds.add(table.id)
            columnIds.addAll(columns.map { it.id })
        }

        removeNonexistentTablesAndColumnsForOrg(orgId, tableIds, columnIds)

        logger.info("Finished syncing database for organization {} in {} ms", orgId, sw.elapsed(TimeUnit.MILLISECONDS))
    }

    private fun initializeTablePermissions(
            organizationId: UUID,
            table: ExternalTable,
            columns: Set<ExternalColumn>,
            adminRolePrincipal: Principal
    ) {
        // initialize database permissions
        extDbPermsService.initializeExternalTablePermissions(
                organizationId,
                table,
                columns
        )
        val columnAcls = columns.map {
            Acl(it.getAclKey(), listOf(Ace(adminRolePrincipal, EnumSet.allOf(Permission::class.java))))
        }
        val tableColsByAclKey = columns.associate {
            it.getAclKey() to TableColumn(it.organizationId, it.tableId, it.id, Schemas.fromName(table.schema))
        }
        extDbPermsService.updateExternalTablePermissions(Action.ADD, columnAcls, tableColsByAclKey)

        // initialize OL permissions
        val acls = edms.syncPermissions(adminRolePrincipal, table, columns)

        // audit
        recordAuditableEvents(acls, AuditEventType.ADD_PERMISSION)
    }

    private fun getOrCreateTable(orgId: UUID, oid: Long, tableName: String, schemaName: String): ExternalTable {
        val table = ExternalTable(
                Optional.empty(),
                tableName,
                tableName,
                Optional.empty(),
                orgId,
                oid,
                schemaName
        )

        val uniqueName = table.getUniqueName()
        if (reservationService.isReserved(uniqueName)) {
            return organizationExternalDatabaseTables.getValue(reservationService.getId(uniqueName))
        }

        createSecurableTableObject(orgId, table)

        return table
    }

    private fun createSecurableTableObject(
            orgId: UUID,
            table: ExternalTable
    ): UUID {
        val newTableId = edms.createOrganizationExternalDatabaseTable(orgId, table)

        organizationMetadataEntitySetsService.addDataset(orgId, table)

        //create audit entity set and audit permissions
        ares.createAuditEntitySetForExternalDBTable(table)

        return newTableId
    }

    private fun syncTableColumns(table: ExternalTable): Set<ExternalColumn> {
        val tableCols = edms.getColumnMetadata(table)
        val tableColNames = tableCols.map { it.getUniqueName() }.toSet()

        val existingColumnIdsByName = reservationService.getIdsByFqn(tableColNames)

        return tableCols.groupBy { existingColumnIdsByName.contains(it.getUniqueName()) }.flatMap { (shouldUpdate, cols) ->
            if (cols.isEmpty()) {
                return@flatMap listOf<ExternalColumn>()
            }

            if (shouldUpdate) {
                updateColumns(cols, existingColumnIdsByName)

            } else {
                createColumns(table, cols)

            }
        }.toSet()
    }

    private fun removeNonexistentTablesAndColumnsForOrg(
        orgId: UUID,
        existingTableIds: Set<UUID>,
        existingColumnIds: Set<UUID>
    ) {

        // delete missing tables

        val tableIdsToDelete: Set<UUID> = organizationExternalDatabaseTables
            .keySet(Predicates.equal(ORGANIZATION_ID_INDEX, orgId))
            .filter { !existingTableIds.contains(it) }
            .toSet()

        if (tableIdsToDelete.isNotEmpty()) {
            edms.deleteExternalTableObjects(tableIdsToDelete)
            organizationMetadataEntitySetsService.deleteDatasets(orgId, tableIdsToDelete)
        }


        // delete missing columns

        val columnIdsToDelete = organizationExternalDatabaseColumns
            .values(Predicates.equal(ORGANIZATION_ID_INDEX, orgId))
            .filter { !existingColumnIds.contains(it.id) }
            .groupBy { it.tableId }
            .mapValues { it.value.map { c -> c.id }.toSet() }

        if (columnIdsToDelete.isNotEmpty()) {
            edms.deleteExternalColumnObjects(orgId, columnIdsToDelete)
            organizationMetadataEntitySetsService.deleteDatasetColumns(orgId, columnIdsToDelete)
        }
    }

    private fun createColumns(
            table: ExternalTable,
            columns: List<ExternalColumn>
    ): List<ExternalColumn> {
        createSecurableColumnObjects(columns, table)

        return columns
    }

    private fun updateColumns(
            columns: List<ExternalColumn>,
            existingColumnIdsByName: Map<String, UUID>
    ): List<ExternalColumn> {

        val currentColumnsById = columns.associateBy { existingColumnIdsByName.getValue(it.getUniqueName()) }
        val storedColumns = organizationExternalDatabaseColumns.getAll(existingColumnIdsByName.values.toSet())

        val fullExistingColumns = mutableListOf<ExternalColumn>()

        val updatedColumns = storedColumns.mapNotNull { (id, storedColumn) ->
            val currentColumn = currentColumnsById.getValue(id)
            val updatedColumn = ExternalColumn(
                    id = storedColumn.id,
                    name = currentColumn.name,
                    title = storedColumn.title,
                    description = Optional.of(storedColumn.description),
                    tableId = currentColumn.tableId,
                    organizationId = currentColumn.organizationId,
                    dataType = currentColumn.dataType,
                    primaryKey = currentColumn.primaryKey,
                    ordinalPosition = currentColumn.ordinalPosition
            )

            fullExistingColumns.add(updatedColumn)

            if (updatedColumn == storedColumn) null else updatedColumn
        }.associateBy { it.id }

        if (updatedColumns.isNotEmpty()) {
            organizationExternalDatabaseColumns.putAll(updatedColumns)
        }

        return fullExistingColumns
    }

    private fun createSecurableColumnObjects(
            columns: List<ExternalColumn>,
            table: ExternalTable
    ): Int {
        var totalSynced = 0

        organizationMetadataEntitySetsService.addDatasetColumns(
                table.organizationId,
                edms.getExternalTable(columns.first().tableId),
                columns
        )

        columns.forEach { column ->
            edms.createOrganizationExternalDatabaseColumn(table.organizationId, column)
            totalSynced++
        }

        return totalSynced
    }

    private fun recordAuditableEvents(acls: List<Acl>, eventType: AuditEventType) {
        val events = acls.map {
            AuditableEvent(
                    IdConstants.SYSTEM_ID.id,
                    AclKey(it.aclKey),
                    eventType,
                    "Permissions updated through BackgroundExternalDatabaseSyncingService",
                    Optional.empty(),
                    ImmutableMap.of("aces", it.aces),
                    OffsetDateTime.now(),
                    Optional.empty()
            )
        }.toList()
        auditingManager.recordEvents(events)
    }
}
