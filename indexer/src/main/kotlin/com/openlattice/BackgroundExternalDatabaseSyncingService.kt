package com.openlattice

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableMap
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.auditing.AuditEventType
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditableEvent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.MAX_DURATION_MILLIS
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
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
        private val indexerConfiguration: IndexerConfiguration
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseSyncingService::class.java)
    }

    private val organizationExternalDatabaseColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(hazelcastInstance)
    private val organizationExternalDatabaseTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(hazelcastInstance)
    private val aclKeys = HazelcastMap.ACL_KEYS.getMap(hazelcastInstance)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)
    private val expirationLocks = HazelcastMap.EXPIRATION_LOCKS.getMap(hazelcastInstance)


    init {
        expirationLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = MAX_DURATION_MILLIS)
    fun scavengeExpirationLocks() {
        expirationLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
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

            logger.info("Completed syncing {} database objects in {}",
                    totalSynced,
                    timer)
        } finally {
            taskLock.unlock()
        }
    }

    private fun syncOrganizationDatabases(orgId: UUID): Int {
        var totalSynced = 0
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val orgOwnerIds = edms.getOrganizationOwners(orgId).map { it.id }
        val currentTableIds = mutableSetOf<UUID>()
        val currentColumnIds = mutableSetOf<UUID>()
        val currentColumnNamesByTableName = edms.getColumnNamesByTable(dbName)
        currentColumnNamesByTableName.forEach { (tableName, columnNames) ->
            //check if we had a record of this table name previously
            val tableFQN = FullQualifiedName(orgId.toString(), tableName)
            val tableId = aclKeys[tableFQN.fullQualifiedNameAsString]
            if (tableId == null) {
                //create new securable object for this table
                val newTable = OrganizationExternalDatabaseTable(Optional.empty(), tableName, tableName, Optional.empty(), orgId)
                val newTableId = createSecurableTableObject(dbName, orgOwnerIds, orgId, currentTableIds, newTable)
                totalSynced++

                //create new securable objects for columns in this table
                totalSynced += createSecurableColumnObjects(dbName, orgOwnerIds, orgId, newTable.name, newTableId, Optional.empty(), currentColumnIds)
            } else {
                currentTableIds.add(tableId)
                //check if columns existed previously
                columnNames.forEach {
                    val columnFQN = FullQualifiedName(tableId.toString(), it)
                    val columnId = aclKeys[columnFQN.fullQualifiedNameAsString]
                    if (columnId == null) {
                        //create new securable object for this column
                        totalSynced += createSecurableColumnObjects(dbName, orgOwnerIds, orgId, tableName, tableId, Optional.of(it), currentColumnIds)
                    } else {
                        currentColumnIds.add(columnId)
                    }
                }

            }

        }

        //check if tables have been deleted in the database
        val missingTableIds = organizationExternalDatabaseTables.keys - currentTableIds
        if (missingTableIds.isNotEmpty()) {
            edms.deleteOrganizationExternalDatabaseTableObjects(missingTableIds)
            totalSynced += missingTableIds.size
        }

        //check if columns have been deleted in the database
        val missingColumnIds = organizationExternalDatabaseColumns.keys - currentColumnIds
        val missingColumnsByTable = mutableMapOf<UUID, MutableSet<UUID>>()
        if (missingColumnIds.isNotEmpty()) {
            missingColumnIds.forEach {
                val tableId = organizationExternalDatabaseColumns.getValue(it).tableId
                missingColumnsByTable.getOrPut(tableId) { mutableSetOf() }.add(it)
            }
            edms.deleteOrganizationExternalDatabaseColumnObjects(missingColumnsByTable)
            totalSynced += missingColumnIds.size
        }
        return totalSynced
    }

    private fun createSecurableTableObject(
            dbName: String,
            orgOwnerIds: List<UUID>,
            orgId: UUID,
            currentTableIds: MutableSet<UUID>,
            table: OrganizationExternalDatabaseTable
    ): UUID {
        val newTableId = edms.createOrganizationExternalDatabaseTable(orgId, table)
        currentTableIds.add(newTableId)

        //add table-level permissions
        val acls = edms.syncPermissions(dbName, orgOwnerIds, orgId, newTableId, table.name, Optional.empty(), Optional.empty())

        //create audit entity set and audit permissions
        ares.createAuditEntitySetForExternalDBTable(table)
        val events = createAuditableEvents(acls, AuditEventType.ADD_PERMISSION)
        auditingManager.recordEvents(events)
        return newTableId
    }

    private fun createSecurableColumnObjects(
            dbName: String,
            orgOwnerIds: List<UUID>,
            orgId: UUID,
            tableName: String,
            tableId: UUID,
            columnName: Optional<String>,
            currentColumnIds: MutableSet<UUID>
    ): Int {
        var totalSynced = 0
        edms.getColumnMetadata(dbName, tableName, tableId, orgId, columnName)
                .forEach { column ->
                    createSecurableColumnObject(dbName, orgOwnerIds, orgId, tableName, currentColumnIds, column)
                    totalSynced++
                }
        return totalSynced
    }

    private fun createSecurableColumnObject(
            dbName: String,
            orgOwnerIds: List<UUID>,
            orgId: UUID,
            tableName: String,
            currentColumnIds: MutableSet<UUID>,
            column: OrganizationExternalDatabaseColumn
    ) {
        val newColumnId = edms.createOrganizationExternalDatabaseColumn(orgId, column)
        currentColumnIds.add(newColumnId)

        //add and audit column-level permissions
        val acls = edms.syncPermissions(dbName, orgOwnerIds, orgId, column.tableId, tableName, Optional.of(newColumnId), Optional.of(column.name))
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