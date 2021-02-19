package com.openlattice

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMap
import com.hazelcast.config.IndexType
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.auditing.AuditEventType
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditableEvent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastMap.Companion.ORGANIZATION_DATABASES
import com.openlattice.hazelcast.HazelcastMap.Companion.ORGANIZATION_EXTERNAL_DATABASE_COLUMN
import com.openlattice.hazelcast.HazelcastMap.Companion.ORGANIZATION_EXTERNAL_DATABASE_TABLE
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.locks.ReentrantLock

/**
 *
 */


class BackgroundExternalDatabaseSyncingService(
        hazelcastInstance: HazelcastInstance,
        private val edms: ExternalDatabaseManagementService,
        private val extDbPermsService: ExternalDatabasePermissioningService,
        private val auditingManager: AuditingManager,
        private val ares: AuditRecordEntitySetsManager,
        private val indexerConfiguration: IndexerConfiguration,
        private val organizationMetadataEntitySetsService: OrganizationMetadataEntitySetsService,
        private val reservationService: HazelcastAclKeyReservationService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseSyncingService::class.java)

        const val MAX_DURATION_MILLIS = 1_000L * 60 * 30 // 30 minutes
        const val SCAN_RATE = 1_000L * 30                // 30 seconds
    }

    private val organizationExternalDatabaseColumns = ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(hazelcastInstance)
    private val organizationExternalDatabaseTables = ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcastInstance)
    private val organizationDatabases = ORGANIZATION_DATABASES.getMap(hazelcastInstance)

    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)
    private val expirationLocks = HazelcastMap.EXPIRATION_LOCKS.getMap(hazelcastInstance)


    init {
        expirationLocks.addIndex(IndexType.SORTED, QueryConstants.THIS_ATTRIBUTE_NAME.value())
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = MAX_DURATION_MILLIS)
    fun scavengeExpirationLocks() {
        expirationLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                )
        )
    }

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
            val lockedOrganizationIds = organizations.keys
                    .filter { it != IdConstants.GLOBAL_ORGANIZATION_ID.id }
                    .filter { tryLockOrganization(it) }
                    .shuffled()

            lockedOrganizationIds
                    .forEach {
                        syncOrganizationDatabases(it)
                    }

            lockedOrganizationIds.forEach(this::deleteLock)

            logger.info("Completed syncing database objects in {}", timer)
        } catch (ex: Exception) {
            logger.error("Failed while syncing external database metadata", ex)
        } finally {
            taskLock.unlock()
        }
    }

    private fun syncOrganizationDatabases(orgId: UUID) {
        val dbName = organizationDatabases[orgId]?.name

        if (dbName == null) {
            logger.error("Organization {} does not exist in the organizationDatabases mapstore", orgId)
            return
        }

        val adminRoleAclKey = organizations.getValue(orgId).adminRoleAclKey

        val tablesToCols = edms.getColumnNamesByTableName(dbName).associate { (oid, tableName, schemaName, _) ->

            val table = getOrCreateTable(orgId, oid, tableName, schemaName, adminRoleAclKey)
            val columns = syncTableColumns(table, adminRoleAclKey)

            extDbPermsService.initializeExternalTablePermissions(
                    orgId,
                    table,
                    columns
            )
            table.id to columns
        }

        removeNonexistentTablesAndColumnsForOrg(orgId, tablesToCols.keys, tablesToCols.flatMap {
            it.value.map { col -> col.id }
        }.toSet())
    }

    private fun getOrCreateTable(orgId: UUID, oid: Int, tableName: String, schemaName: String, adminRoleAclKey: AclKey): OrganizationExternalDatabaseTable {
        val table = OrganizationExternalDatabaseTable(
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

        createSecurableTableObject(adminRoleAclKey, orgId, table)

        return table
    }

    private fun createSecurableTableObject(
            adminRoleAclKey: AclKey,
            orgId: UUID,
            table: OrganizationExternalDatabaseTable
    ): UUID {
        val newTableId = edms.createOrganizationExternalDatabaseTable(orgId, table)

        //add table-level permissions
        val acls = edms.syncPermissions(adminRoleAclKey, table)

        organizationMetadataEntitySetsService.addDataset(orgId, table)

        //create audit entity set and audit permissions
        ares.createAuditEntitySetForExternalDBTable(table)
        val events = createAuditableEvents(acls, AuditEventType.ADD_PERMISSION)
        auditingManager.recordEvents(events)

        return newTableId
    }

    private fun syncTableColumns(table: OrganizationExternalDatabaseTable, adminRoleAclKey: AclKey): Set<OrganizationExternalDatabaseColumn> {
        val tableCols = edms.getColumnMetadata(table)
        val tableColNames = tableCols.map { it.getUniqueName() }.toSet()

        val existingColumnIdsByName = reservationService.getIdsByFqn(tableColNames)

        return tableCols.groupBy { existingColumnIdsByName.contains(it.getUniqueName()) }.flatMap { (shouldUpdate, cols) ->
            if (cols.isEmpty()) {
                return@flatMap listOf<OrganizationExternalDatabaseColumn>()
            }

            if (shouldUpdate) {
                updateColumns(cols, existingColumnIdsByName)

            } else {
                createColumns(table, cols, adminRoleAclKey)

            }
        }.toSet()
    }

    private fun removeNonexistentTablesAndColumnsForOrg(orgId: UUID, existingTableIds: Set<UUID>, existingColumnIds: Set<UUID>) {

        // delete missing tables

        val tableIdsToDelete = organizationExternalDatabaseTables.keySet(Predicates.equal(ORGANIZATION_ID_INDEX, orgId)).filter {
            !existingTableIds.contains(it)
        }.toSet()

        if (tableIdsToDelete.isNotEmpty()) {
            edms.deleteOrganizationExternalDatabaseTableObjects(tableIdsToDelete)
        }


        // delete missing columns

        val columnIdsToDelete = organizationExternalDatabaseColumns.values(Predicates.equal(ORGANIZATION_ID_INDEX, orgId)).filter {
            !existingColumnIds.contains(it.id)
        }.groupBy { it.tableId }.mapValues { it.value.map { c -> c.id }.toSet() }

        if (columnIdsToDelete.isNotEmpty()) {
            edms.deleteOrganizationExternalDatabaseColumnObjects(columnIdsToDelete)
        }
    }

    private fun createColumns(
            table: OrganizationExternalDatabaseTable,
            columns: List<OrganizationExternalDatabaseColumn>,
            adminRoleAclKey: AclKey
    ): List<OrganizationExternalDatabaseColumn> {
        createSecurableColumnObjects(columns, adminRoleAclKey, table)

        return columns
    }

    private fun updateColumns(
            columns: List<OrganizationExternalDatabaseColumn>,
            existingColumnIdsByName: Map<String, UUID>
    ): List<OrganizationExternalDatabaseColumn> {

        val currentColumnsById = columns.associateBy { existingColumnIdsByName.getValue(it.getUniqueName()) }
        val storedColumns = organizationExternalDatabaseColumns.getAll(existingColumnIdsByName.values.toSet())

        val fullExistingColumns = mutableListOf<OrganizationExternalDatabaseColumn>()

        val updatedColumns = storedColumns.mapNotNull { (id, storedColumn) ->
            val currentColumn = currentColumnsById.getValue(id)
            val updatedColumn = OrganizationExternalDatabaseColumn(
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

    private fun deleteColumns(columns: List<OrganizationExternalDatabaseColumn>): Int {
        edms.deleteOrganizationExternalDatabaseColumnObjects(columns
                .groupBy { it.tableId }
                .mapValues { it.value.map { c -> c.id }.toSet() }
        )

        return columns.size
    }

    private fun createSecurableColumnObjects(
            columns: List<OrganizationExternalDatabaseColumn>,
            adminRoleAclKey: AclKey,
            table: OrganizationExternalDatabaseTable
    ): Int {
        var totalSynced = 0

        organizationMetadataEntitySetsService.addDatasetColumns(
                table.organizationId,
                edms.getOrganizationExternalDatabaseTable(columns.first().tableId),
                columns
        )

        columns.forEach { column ->
            createSecurableColumnObject(adminRoleAclKey, table, column)
            totalSynced++
        }
        return totalSynced
    }

    private fun createSecurableColumnObject(
            adminRoleAclKey: AclKey,
            table: OrganizationExternalDatabaseTable,
            column: OrganizationExternalDatabaseColumn
    ) {
        val newColumnId = edms.createOrganizationExternalDatabaseColumn(table.organizationId, column)

        //add and audit column-level permissions and postgres privileges
        val acls = edms.syncPermissions(
                adminRoleAclKey,
                table,
                newColumnId,
                column.name
        )


        val events = createAuditableEvents(acls, AuditEventType.ADD_PERMISSION)
        auditingManager.recordEvents(events)
    }

    private fun createAuditableEvents(acls: List<Acl>, eventType: AuditEventType): List<AuditableEvent> {
        return acls.map {
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
    }

    private fun tryLockOrganization(orgId: UUID): Boolean {
        return expirationLocks.putIfAbsent(orgId, System.currentTimeMillis() + MAX_DURATION_MILLIS) == null
    }

    private fun deleteLock(orgId: UUID) {
        expirationLocks.delete(orgId)
    }

}