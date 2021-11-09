/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */
package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.LAST_LINK_INDEX
import com.openlattice.postgres.PostgresColumn.LINKING_ID
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.PostgresTable.SYNC_IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.rhizome.DelegatedIntSet
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock


const val DELETE_SIZE = 10_000
const val DELETE_RATE = 30_000L

/**
 * Background service to delete entities from ids table, which have been hard deleted and already processed by
 * background (un-)indexing tasks.
 */
class BackgroundIndexedEntitiesDeletionService(
        hazelcastInstance: HazelcastInstance,
        private val dataSourceResolver: DataSourceResolver,
        private val indexerConfiguration: IndexerConfiguration,
        private val dataQueryService: PostgresEntityDataQueryService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundIndexedEntitiesDeletionService::class.java)
    }

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val deletedEntitySets = HazelcastMap.DELETED_ENTITY_SETS.getMap(hazelcastInstance)

    private val taskLock = ReentrantLock()

    @Suppress("UNUSED")
    @Scheduled(fixedRate = DELETE_RATE)
    fun deleteIndexedEntitiesInEntitySet() {
        logger.info("Starting background deletion task.")

        //Keep number of deletion jobs under control
        if (!taskLock.tryLock()) {
            logger.info("Not starting new deletion job as an existing one is running.")
            return
        }

        try {
            if (!indexerConfiguration.backgroundDeletionEnabled) {
                logger.info("Skipping background deletion as it is not enabled.")
                return
            }

            val w = Stopwatch.createStarted()

            //We shuffle entity sets to make sure we have a chance to work share and index everything
            val totalCurrentEntitySetUpdates = entitySets.values
                    .filter { !it.isAudit }
                    .map { EntitySetForDeletion(it.id, it.name, it.partitions) }
                    .shuffled()
                    .map {
                        try {
                            processDeletedEntitiesForEntitySet(it, true)
                        } catch (e: Exception) {
                            logger.error("An error occurred while deleting indexed entities for entity set {}", it.id, e)
                            0
                        }
                    }.sum()

            val totalDeletedEntitySetUpdates = (deletedEntitySets as Map<UUID, DelegatedIntSet>)
                    .map { (id, partitions) -> EntitySetForDeletion(id, "Deleted entity set [$id]", partitions) }
                    .shuffled()
                    .map {
                        try {
                            val numUpdates = processDeletedEntitiesForEntitySet(it, false)
                            this.deletedEntitySets.delete(it.id)
                            numUpdates
                        } catch (e: Exception) {
                            logger.error("An error occurred while deleting entities for entity set {}", it.id, e)

                            0
                        }
                    }.sum()

            logger.info(
                    "Completed deletion of {} entities in {} ms",
                    totalCurrentEntitySetUpdates + totalDeletedEntitySetUpdates,
                    w.elapsed(TimeUnit.MILLISECONDS)
            )
        } finally {
            taskLock.unlock()
        }
    }

    private fun processDeletedEntitiesForEntitySet(entitySet: EntitySetForDeletion, isCurrentEntitySet: Boolean): Int {
        logger.debug("Starting entity deletion for entity set ${entitySet.name} with id ${entitySet.id}")

        val esw = Stopwatch.createStarted()
        var deletableIds = getDeletedIdsBatch(entitySet, isCurrentEntitySet).toSet()

        var deleteCount = 0

        while (deletableIds.isNotEmpty()) {
            deleteCount += dataQueryService.deleteEntities(entitySet.id, deletableIds, entitySet.partitions).numUpdates
            deleteFromSyncIds(entitySet.id, deletableIds)
            deletableIds = getDeletedIdsBatch(entitySet, isCurrentEntitySet).toSet()
        }

        logger.debug(
                "Finished deleting $deleteCount elements from entity set ${entitySet.name} in " +
                        "${esw.elapsed(TimeUnit.MILLISECONDS)} ms."
        )

        return deleteCount
    }

    /**
     * Select all ids, which have been hard deleted and already un-indexed.
     */
    private fun getDeletedIdsBatch(
            entitySet: EntitySetForDeletion,
            isCurrentEntitySet: Boolean = true
    ): BasePostgresIterable<UUID> {
        val sql = if (isCurrentEntitySet) selectCurrentEntitySetIdsBatch else selectDeletedEntitySetIdsBatch
        val hds = dataSourceResolver.resolve(entitySet.id)
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, sql) {
                    val partitionsArray = PostgresArrays.createIntArray(it.connection, entitySet.partitions)
                    it.setObject(1, entitySet.id)
                    it.setArray(2, partitionsArray)
                }
        ) { rs -> ResultSetAdapters.id(rs) }
    }

    private val selectDeletedEntitySetIdsBatch = """
        SELECT ${ID.name}
        FROM ${IDS.name}
        WHERE
          ${ENTITY_SET_ID.name} = ?
          AND ${PARTITION.name} = ANY(?)
        LIMIT $DELETE_SIZE
    """.trimIndent()


    // get all ids where version = 0 and either
    // a: last_index >= last_write
    // b: last_link_index >= last_write and linking_id is not null
    private val selectCurrentEntitySetIdsBatch =
            """
                SELECT ${ID.name}
                FROM ${IDS.name}
                WHERE
                  ${ENTITY_SET_ID.name} = ? AND
                  ${PARTITION.name} = ANY(?) AND
                  ${VERSION.name} = 0 AND
                  (
                    (${LAST_INDEX.name} >= ${LAST_WRITE.name})
                    OR
                    (${LAST_LINK_INDEX.name} >= ${LAST_WRITE.name} AND ${LINKING_ID.name} IS NOT NULL)
                  )
                  LIMIT $DELETE_SIZE
            """.trimIndent()

    private fun deleteFromSyncIds(entitySetId: UUID, entityKeyIds: Set<UUID>) {
        val hds = dataSourceResolver.resolve(entitySetId)
        hds.connection.use { conn ->
            conn.prepareStatement(deleteFromSyncIdsSql).use { ps ->
                ps.setObject(1, entitySetId)
                ps.setArray(2, PostgresArrays.createUuidArray(conn, entityKeyIds))
                ps.executeUpdate()
            }
        }
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     * 2) entityKeyIds
     */
    private val deleteFromSyncIdsSql = """
        DELETE FROM ${SYNC_IDS.name}
        WHERE
          ${ENTITY_SET_ID.name} = ?
          AND ${ID.name} = ANY(?)
    """.trimIndent()

    data class EntitySetForDeletion(
            val id: UUID,
            val name: String,
            val partitions: Set<Int>
    )
}