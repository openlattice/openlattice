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
import com.openlattice.indexing.MAX_DURATION_MILLIS
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.organizations.mapstores.OrganizationExternalDatabaseTableMapstore
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.locks.ReentrantLock

/**
 *
 */

const val SCAN_RATE = 30_000L

class BackgroundExternalDatabaseSyncingService(
        hazelcastInstance: HazelcastInstance,
        private val edms: ExternalDatabaseManagementService,
        private val auditingManager: AuditingManager,
        private val ares: AuditRecordEntitySetsManager,
        private val indexerConfiguration: IndexerConfiguration,
        private val organizationMetadataEntitySetsService: OrganizationMetadataEntitySetsService,
        private val reservationService: HazelcastAclKeyReservationService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseSyncingService::class.java)
    }

    private val organizationExternalDatabaseColumns = ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(hazelcastInstance)
    private val organizationExternalDatabaseTables = ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcastInstance)
    private val organizationDatabases = ORGANIZATION_DATABASES.getMap(hazelcastInstance)

    private val aclKeys = HazelcastMap.ACL_KEYS.getMap(hazelcastInstance)
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
    @Scheduled(fixedRate = SCAN_RATE)
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
                    .parallelStream()
                    .forEach {
                        syncOrganizationDatabases(it)
                    }

            lockedOrganizationIds.forEach(this::deleteIndexingLock)

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

        val orgOwnerAclKeys = edms.getOrganizationOwners(orgId).map { it.aclKey }

        val tablesToCols = edms.getColumnNamesByTableName(dbName).associate { (oid, tableName, schemaName, columnNames) ->

            val table = getOrCreateTable(orgId, oid, tableName, schemaName)
            val columnIds = syncTableColumns(table, orgOwnerAclKeys)

            table.id to columnIds
        }

        removeNonexistentTablesAndColumnsForOrg(orgId, tablesToCols.keys, tablesToCols.flatMap { it.value }.toSet())

    }

    private fun getOrCreateTable(orgId: UUID, oid: Int, tableName: String, schemaName: String): OrganizationExternalDatabaseTable {
        val table = OrganizationExternalDatabaseTable(
                Optional.empty(),
                tableName,
                tableName,
                Optional.empty(),
                orgId,
                oid,
                schemaName
        )

        if (reservationService.isReserved(table.getUniqueName())) {
            return organizationExternalDatabaseTables.getValue(reservationService.getId(table.getUniqueName()))
        }

        return table
    }

    private fun syncTableColumns(table: OrganizationExternalDatabaseTable, ownerAclKeys: List<AclKey>): Set<UUID> {
        val tableCols = edms.getColumnMetadata(table.name, table.id, table.organizationId)
        val tableColNames = tableCols.map { it.getUniqueName() }.toSet()

        val existingColumnIdsByName = reservationService.getIdsByFqn(tableColNames)

        return tableCols.groupBy { existingColumnIdsByName.contains(it.getUniqueName()) }.flatMap { (shouldUpdate, cols) ->
            if (cols.isEmpty()) {
                return@flatMap listOf<OrganizationExternalDatabaseColumn>()
            }

            if (shouldUpdate) {
                updateColumns(cols, existingColumnIdsByName)
            } else {
                createColumns(table, cols, ownerAclKeys)
            }
        }.map { it.id }.toSet()
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
            ownerAclKeys: List<AclKey>
    ): List<OrganizationExternalDatabaseColumn> {
        createSecurableColumnObjects(columns, ownerAclKeys, table)

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
            orgOwnerAclKeys: List<AclKey>,
            table: OrganizationExternalDatabaseTable
    ): Int {
        var totalSynced = 0

        organizationMetadataEntitySetsService.addDatasetColumns(
                table.organizationId,
                edms.getOrganizationExternalDatabaseTable(columns.first().tableId),
                columns
        )

        columns.forEach { column ->
            createSecurableColumnObject(orgOwnerAclKeys, table, column)
            totalSynced++
        }
        return totalSynced
    }

    private fun createSecurableColumnObject(
            orgOwnerAclKeys: List<AclKey>,
            table: OrganizationExternalDatabaseTable,
            column: OrganizationExternalDatabaseColumn
    ) {
        val newColumnId = edms.createOrganizationExternalDatabaseColumn(table.organizationId, column)

        //add and audit column-level permissions and postgres privileges
        val acls = edms.syncPermissions(
                orgOwnerAclKeys,
                table,
                Optional.of(newColumnId),
                Optional.of(column.name)
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

    private fun deleteIndexingLock(orgId: UUID) {
        expirationLocks.delete(orgId)
    }

}