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
        private val organizationMetadataEntitySetsService: OrganizationMetadataEntitySetsService
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

            val totalSynced = lockedOrganizationIds
                    .parallelStream()
                    .mapToInt {
                        syncOrganizationDatabases(it)
                    }
                    .sum()

            lockedOrganizationIds.forEach(this::deleteIndexingLock)

            logger.info(
                    "Completed syncing {} database objects in {}",
                    totalSynced,
                    timer
            )
        } finally {
            taskLock.unlock()
        }
    }

    private fun syncOrganizationDatabases(orgId: UUID): Int {
        var totalSynced = 0
        val dbName = organizationDatabases.getValue(orgId).name
        val orgOwnerIds = edms.getOrganizationOwners(orgId).map { it.id }
        val currentTableIds = mutableSetOf<UUID>()
        val currentColumnIds = mutableSetOf<UUID>()
        val currentColumnNamesByTableName = edms.getColumnNamesByTableName(dbName)
        currentColumnNamesByTableName.forEach { (tableName, schemaInfo) ->
            //check if we had a record of this table name previously
            val tableFQN = FullQualifiedName(orgId.toString(), tableName)
            val tableId = aclKeys[tableFQN.fullQualifiedNameAsString]
            if (tableId == null) {
                //create new securable object for this table
                val newTable = OrganizationExternalDatabaseTable(
                        Optional.empty(),
                        tableName,
                        tableName,
                        Optional.empty(),
                        orgId,
                        schemaInfo.oid
                )
                val newTableId = createSecurableTableObject(orgOwnerIds, orgId, currentTableIds, newTable)
                totalSynced++

                //create new securable objects for columns in this table
                totalSynced += createSecurableColumnObjects(
                        orgOwnerIds,
                        orgId,
                        newTable.name,
                        newTableId,
                        Optional.empty(),
                        currentColumnIds
                )
            } else {
                currentTableIds.add(tableId)
                //check if columns existed previously
                schemaInfo.columnNames.forEach {
                    val columnFQN = FullQualifiedName(tableId.toString(), it)
                    val columnId = aclKeys[columnFQN.fullQualifiedNameAsString]
                    if (columnId == null) {
                        //create new securable object for this column
                        totalSynced += createSecurableColumnObjects(
                                orgOwnerIds,
                                orgId,
                                tableName,
                                tableId,
                                Optional.of(it),
                                currentColumnIds
                        )
                    } else {
                        currentColumnIds.add(columnId)
                    }
                }

            }

        }

        //check if tables have been deleted in the database
        val missingTableIds = organizationExternalDatabaseTables
                .filter { it.value.organizationId == orgId }
                .keys - currentTableIds
        if (missingTableIds.isNotEmpty()) {
            edms.deleteOrganizationExternalDatabaseTableObjects(missingTableIds)
            totalSynced += missingTableIds.size
        }

        //check if columns have been deleted in the database
        val missingColumnIds = organizationExternalDatabaseColumns
                .filter { it.value.organizationId == orgId }
                .keys - currentColumnIds


        if (missingColumnIds.isNotEmpty()) {
            val missingColumnsByTable = organizationExternalDatabaseColumns
                    .getAll(missingColumnIds).entries
                    .groupBy { it.value.tableId }
                    .mapValues { it.value.map { entry -> entry.key!! }.toSet() }

            edms.deleteOrganizationExternalDatabaseColumnObjects(missingColumnsByTable)
            totalSynced += missingColumnIds.size
        }

        return totalSynced
    }

    private fun createSecurableTableObject(
            orgOwnerIds: List<UUID>,
            orgId: UUID,
            currentTableIds: MutableSet<UUID>,
            table: OrganizationExternalDatabaseTable
    ): UUID {
        val newTableId = edms.createOrganizationExternalDatabaseTable(orgId, table)
        currentTableIds.add(newTableId)

        //add table-level permissions
        val acls = edms.syncPermissions(
                orgOwnerIds,
                orgId,
                newTableId,
                table.name,
                Optional.empty(),
                Optional.empty()
        )

        organizationMetadataEntitySetsService.addDataset(orgId, table.oid, table.id, table.name)

        //create audit entity set and audit permissions
        ares.createAuditEntitySetForExternalDBTable(table)
        val events = createAuditableEvents(acls, AuditEventType.ADD_PERMISSION)
        auditingManager.recordEvents(events)

        return newTableId
    }

    private fun createSecurableColumnObjects(
            orgOwnerIds: List<UUID>,
            orgId: UUID,
            tableName: String,
            tableId: UUID,
            columnName: Optional<String>,
            currentColumnIds: MutableSet<UUID>
    ): Int {
        var totalSynced = 0
        val columns = edms.getColumnMetadata(tableName, tableId, orgId, columnName).toList()

        organizationMetadataEntitySetsService.addDatasetColumns(
                orgId,
                edms.getOrganizationExternalDatabaseTable(columns.first().tableId),
                columns
        )

        columns.forEach { column ->
            createSecurableColumnObject(orgOwnerIds, orgId, tableName, currentColumnIds, column)
            totalSynced++
        }
        return totalSynced
    }

    private fun createSecurableColumnObject(
            orgOwnerIds: List<UUID>,
            orgId: UUID,
            tableName: String,
            currentColumnIds: MutableSet<UUID>,
            column: OrganizationExternalDatabaseColumn
    ) {
        val newColumnId = edms.createOrganizationExternalDatabaseColumn(orgId, column)
        currentColumnIds.add(newColumnId)

        //add and audit column-level permissions and postgres privileges
        val acls = edms.syncPermissions(
                orgOwnerIds,
                orgId,
                column.tableId,
                tableName,
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