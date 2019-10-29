package com.openlattice

import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.MAX_DURATION_MILLIS
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*
import java.util.concurrent.locks.ReentrantLock

/**
 *
 */

const val SCAN_RATE = 30_000L

class BackgroundExternalDatabaseSyncingService(
        private val hazelcastInstance: HazelcastInstance,
        private val edms: ExternalDatabaseManagementService,
        private val indexerConfiguration: IndexerConfiguration
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseSyncingService::class.java)
    }

    private val organizationExternalDatabaseColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COlUMN.name)
    private val organizationExternalDatabaseTables: IMap<UUID, OrganizationExternalDatabaseTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.name)
    private val aclKeys: IMap<String, UUID> = hazelcastInstance.getMap(HazelcastMap.ACL_KEYS.name)
    private val organizationTitles: IMap<UUID, String> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATIONS_TITLES.name)
    private val expirationLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.EXPIRATION_LOCKS.name)


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
        if (taskLock.tryLock()) {
            //add enabling config
            try {
                if (indexerConfiguration.backgroundExternalDatabaseSyncingEnabled) {
                    val timer = Stopwatch.createStarted()
                    val lockedOrganizationIds = organizationTitles.keys
                            .filter { it != IdConstants.OPENLATTICE_ORGANIZATION_ID.id }
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
                } else {
                    logger.info("Skipping external database syncing as it is not enabled.")
                }
            } finally {
                taskLock.unlock()
            }
        } else {
            logger.info("Not starting new external database sync task as an existing one is running")
        }
    }

    fun syncOrganizationDatabases(orgId: UUID): Int {
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val currentTableIds = mutableSetOf<UUID>()
        val currentColumnIds = mutableSetOf<UUID>()
        val currentColumnNamesByTableName = edms.getColumnNamesByTable(orgId, dbName)
        currentColumnNamesByTableName.forEach { (tableName, columnNames) ->
            //check if table existed previously
            val tableFQN = FullQualifiedName(orgId.toString(), tableName)
            val tableId = aclKeys[tableFQN.fullQualifiedNameAsString]
            if (tableId == null) {
                //create new securable object for this table
                val newTable = OrganizationExternalDatabaseTable(Optional.empty(), tableName, tableName, Optional.empty(), orgId)
                val newTableId = edms.createOrganizationExternalDatabaseTable(orgId, newTable)
                currentTableIds.add(newTableId)

                //add table-level permissions
                edms.addPermissions(dbName, orgId, newTableId, newTable.name, Optional.empty(), Optional.empty())

                //create new securable objects for columns in this table
                edms.createNewColumnObjects(dbName, tableName, newTableId, orgId, Optional.empty())
                        .forEach {
                    val newColumnId = edms.createOrganizationExternalDatabaseColumn(orgId, it)
                    currentColumnIds.add(newColumnId)

                    //add column-level permissions
                    edms.addPermissions(dbName, orgId, newTableId, newTable.name, Optional.of(newColumnId), Optional.of(it.name))
                }
            } else {
                currentTableIds.add(tableId)
                //check if columns existed previously
                columnNames.forEach {
                    val columnFQN = FullQualifiedName(tableId.toString(), it)
                    val columnId = aclKeys[columnFQN.fullQualifiedNameAsString]
                    if (columnId == null) {
                        //create new securable object for this column
                        edms.createNewColumnObjects(dbName, tableName, tableId, orgId, Optional.of(it))
                                .forEach { column ->
                            val newColumnId = edms.createOrganizationExternalDatabaseColumn(orgId, column)
                            currentColumnIds.add(newColumnId)

                            //add column-level permissions
                            edms.addPermissions(dbName, orgId, tableId, tableName, Optional.of(newColumnId), Optional.of(column.name))
                        }
                    } else {
                        currentColumnIds.add(columnId)
                    }
                }

            }

        }

        //check if tables have been deleted in the database
        val missingTableIds = organizationExternalDatabaseTables.keys - currentTableIds
        if (missingTableIds.isNotEmpty()) {
            edms.deleteOrganizationExternalDatabaseTables(orgId, missingTableIds)
        }

        //check if columns have been deleted in the database
        val missingColumnIds = organizationExternalDatabaseColumns.keys - currentColumnIds
        if (missingTableIds.isNotEmpty()) {
            edms.deleteOrganizationExternalDatabaseColumns(orgId, missingColumnIds)
        }
        return 1 //TODO add actual tracking of counts
    }

    private fun tryLockOrganization(orgId: UUID): Boolean {
        return expirationLocks.putIfAbsent(orgId, System.currentTimeMillis() + MAX_DURATION_MILLIS) == null
    }

    private fun deleteIndexingLock(orgId: UUID) {
        expirationLocks.delete(orgId)
    }

}