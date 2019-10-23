package com.openlattice

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.MAX_DURATION_MILLIS
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*

/**
 *
 */
class BackgroundExternalDatabaseUpdatingService(
        private val hazelcastInstance: HazelcastInstance,
        private val edms: ExternalDatabaseManagementService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseUpdatingService::class.java)
    }

    private val organizationExternalDatabaseColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COlUMN.name)
    private val organizationExternalDatabaseTables: IMap<UUID, OrganizationExternalDatabaseTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.name)
    private val aclKeys: IMap<String, UUID> = hazelcastInstance.getMap(HazelcastMap.ACL_KEYS.name)

    @Suppress("UNUSED")
    @Scheduled(fixedRate = MAX_DURATION_MILLIS)
    fun scanOrganizationDatabases() {
        val orgIds = edms.getOrganizationIds()
        orgIds.forEach {orgId ->
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

                    val newColumns = edms.createNewColumnObjects(dbName, tableName, newTableId, orgId, Optional.empty())
                    newColumns.forEach {
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
                            //TODO handling of permissions
                            val newColumn = edms.createNewColumnObjects(dbName, tableName, tableId, orgId, Optional.of(it))
                            newColumn.forEach { newColumn ->
                                val newColumnId = edms.createOrganizationExternalDatabaseColumn(orgId, newColumn)
                                currentColumnIds.add(newColumnId)

                                //add column-level permissions
                                edms.addPermissions(dbName, orgId, tableId, tableName, Optional.of(newColumnId), Optional.of(newColumn.name))
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

        }


    }

}