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
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock


const val DELETE_SIZE = 10_000

/**
 * Background service to delete entities from ids table, which have been hard deleted and already processed by
 * background (un-)indexing tasks.
 */
class BackgroundIndexedEntitiesDeletionService(
        hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        private val indexerConfiguration: IndexerConfiguration,
        private val dataQueryService: PostgresEntityDataQueryService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundIndexedEntitiesDeletionService::class.java)
    }

    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)

    private val deletionLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.DELETION_LOCKS.name)

    init {
        deletionLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = EXPIRATION_MILLIS)
    fun scavengeIndexingLocks() {
        deletionLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    @Suppress("UNUSED")
    @Scheduled(fixedRate = INDEX_RATE)
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
            val lockedEntitySets = entitySets.values
                    .shuffled()
                    .filter { tryLockEntitySet(it.id) }
                    .filter { it.name != "OpenLattice Audit Entity Set" } //TODO: Clean out audit entity set from prod

            val totalDeleted = lockedEntitySets
                    .parallelStream()
                    .mapToInt { deleteIndexedEntities(it) }
                    .sum()

            lockedEntitySets.forEach { deleteDeletionLock(it.id) }

            logger.info(
                    "Completed deletion of {} entities in {} ms",
                    totalDeleted,
                    w.elapsed(TimeUnit.MILLISECONDS)
            )
        } finally {
            taskLock.unlock()
        }
    }

    private fun deleteIndexedEntities(entitySet: EntitySet): Int {
        logger.info("Starting entity deletion for entity set ${entitySet.name} with id ${entitySet.id}")

        val esw = Stopwatch.createStarted()
        val deletableIds = getDeletedIds(entitySet.id)

        var deleteCount = 0
        deletableIds.asSequence().chunked(DELETE_SIZE).forEach {
            updateExpiration(entitySet.id)
            deleteCount += dataQueryService.deleteEntities(entitySet.id, it.toSet()).numUpdates
        }

        logger.info(
                "Finished deleting $deleteCount elements from entity set ${entitySet.name} in " +
                        "${esw.elapsed(TimeUnit.MILLISECONDS)} ms."
        )

        return deleteCount
    }

    private fun getDeletedIds(entitySetId: UUID): BasePostgresIterable<UUID> {
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, selectDeletedIds(), FETCH_SIZE) {
                    it.setObject(1, entitySetId)
                }
        ) { rs -> ResultSetAdapters.id(rs) }
    }

    private fun selectDeletedIds(): String {
        // get all ids where version = 0 and either
        // a: last_index >= last_write
        // b: last_link_index >= last_write and linking_id is not null
        return "SELECT ${ID.name} " +
                "FROM ${IDS.name} " +
                "WHERE " +
                "${ENTITY_SET_ID.name} = ? AND " +
                "${VERSION.name} = 0 AND" +
                "( OR )"
    }

    private fun tryLockEntitySet(entitySetId: UUID): Boolean {
        return deletionLocks.putIfAbsent(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS) == null
    }

    private fun deleteDeletionLock(entitySetId: UUID) {
        deletionLocks.delete(entitySetId)
    }

    private fun updateExpiration(entitySetId: UUID) {
        deletionLocks.set(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS)
    }
}