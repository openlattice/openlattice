package com.openlattice.organizations.tasks

import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.mapstores.TABLE_ID_INDEX
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.HazelcastTaskDependencies
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit


const val SYNC_INTERVAL_MILLIS = 30_000L
private val logger = LoggerFactory.getLogger(ExternalDatabasePermissionsSyncTask::class.java)

class ExternalDatabasePermissionsSyncTask : HazelcastFixedRateTask<ExternalDatabasePermissionsSyncTaskDependencies>, HazelcastTaskDependencies {
    override fun getDependenciesClass(): Class<ExternalDatabasePermissionsSyncTaskDependencies> {
        return ExternalDatabasePermissionsSyncTaskDependencies::class.java
    }

    override fun getName(): String {
        return Task.EXTERNAL_DATABASE_PERMISSIONS_SYNC_TASK.name
    }

    override fun getInitialDelay(): Long {
        return SYNC_INTERVAL_MILLIS
    }

    override fun getPeriod(): Long {
        return SYNC_INTERVAL_MILLIS
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun runTask() {
        val dependencies = getDependency()
        val externalDBTables: IMap<UUID, OrganizationExternalDatabaseTable> = dependencies.hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.name)
        val externalDBColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = dependencies.hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.name)
        logger.info("Syncing permissions for external databases")
        externalDBTables.forEach { (tableId, table) ->
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(table.organizationId)
            dependencies.externalDBManager.addPermissions(dbName, table.organizationId, tableId, table.name, Optional.empty(), Optional.empty())
            externalDBColumns.entrySet(belongsToTable(tableId)).forEach {
                dependencies.externalDBManager.addPermissions(dbName, table.organizationId, tableId, table.name, Optional.of(it.key), Optional.of(it.value.name))
            }
        }
    }

    private fun belongsToTable(tableId: UUID): Predicate<*, *> {
        return Predicates.equal(TABLE_ID_INDEX, tableId)
    }

}