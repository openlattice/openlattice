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

import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.admin.indexing.IndexingState
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.hazelcast.processors.UUIDKeyToUUIDSetMerger
import com.openlattice.postgres.PostgresColumn.ENTITY_KEY_IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Stream

private val logger = LoggerFactory.getLogger(IndexingService::class.java)
private val LB_UUID = UUID(0, 0)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IndexingService(
        private val hds: HikariDataSource,
        private val backgroundIndexingService: BackgroundIndexingService,
        executor: ListeningExecutorService,
        hazelcastInstance: HazelcastInstance
) {
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val propertyTypes = hazelcastInstance.getMap<UUID, PropertyType>(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes = hazelcastInstance.getMap<UUID, EntityType>(HazelcastMap.ENTITY_TYPES.name)
    private val indexingJobs = hazelcastInstance.getMap<UUID, DelegatedUUIDSet>(HazelcastMap.INDEXING_JOBS.name)
    private val indexingProgress = hazelcastInstance.getMap<UUID, UUID>(HazelcastMap.INDEXING_PROGRESS.name)
    private val indexingQueue = hazelcastInstance.getQueue<UUID>(HazelcastQueue.INDEXING.name)
    private val indexingLock = ReentrantLock()

    init {
        //Add back in jobs that did not complete but are still registered.
        val jobIds = indexingJobs.keys
        jobIds.removeIf(indexingQueue::contains)
        logger.info("Re-adding the following jobs that were registered but not in the queue: {}", jobIds)
        queueForIndexing(jobIds.map { it to emptySet<UUID>() }.toMap())
    }

    private val indexingWorker = executor.submit {
        Stream.generate { indexingQueue.take() }
                .parallel().forEach { entitySetId ->
                    try {
                        val entitySet = entitySets.getValue(entitySetId)
                        val propertyTypeMap = propertyTypes.getAll(
                                entityTypes.getValue(entitySet.entityTypeId).properties
                        )

                        var cursor = indexingProgress.getOrPut(entitySetId) { LB_UUID }
                        var entityKeyIds = indexingJobs[entitySetId] ?: getNextBatch(
                                entitySetId,
                                cursor,
                                cursor != LB_UUID
                        ).toSet()

                        while (entityKeyIds.isNotEmpty()) {
                            logger.info("Indexing entity set ${entitySet.name} ($entitySet.id) starting at $cursor.")
                            backgroundIndexingService.indexEntities(entitySet, entityKeyIds, propertyTypeMap, false)
                            val cursor = entityKeyIds.max()!!

                            indexingProgress.set(entitySetId, cursor)

                            entityKeyIds = getNextBatch(entitySetId, cursor, cursor != LB_UUID).toSortedSet()
                        }

                        logger.info("Finished indexing entity set $entitySetId")
                        //We're done re-indexing this set.
                        try {
                            indexingLock.lock()
                            indexingJobs.delete(entitySetId)
                            indexingProgress.delete(entitySetId)
                        } finally {
                            indexingLock.unlock()
                        }
                    } catch (ex: Exception) {
                        logger.error("Error while marking entity set as needing indexing.", ex)
                    }
                }

    }

    fun getIndexingState(): IndexingState {
        return IndexingState(
                indexingJobs.getAll(indexingJobs.keys),
                indexingQueue,
                indexingQueue.peek(),
                indexingQueue.size
        )
    }

    fun queueForIndexing(entities: Map<UUID, Set<UUID>>): Int {
        val entitiesForIndexing = if (entities.isEmpty()) {
            entitySets.keys.map { it to emptySet<UUID>() }.toMap()
        } else {
            entities
        }

        entitiesForIndexing.forEach { entitySetId, entityKeyIds ->
            logger.info("Creating job to index entity set {} for the following entities {}", entitySetId, entityKeyIds)
            indexingJobs.putIfAbsent(entitySetId, DelegatedUUIDSet.wrap(HashSet()))
            indexingJobs.executeOnKey(entitySetId, UUIDKeyToUUIDSetMerger(entityKeyIds))
            indexingQueue.put(entitySetId)
        }

        return entitiesForIndexing.size
    }

    private fun getNextBatchQuery(entitySetId: UUID, useLowerbound: Boolean): String {
        val lowerboundSql = if (useLowerbound) {
            "AND id > ?"
        } else {
            ""
        }

        return "SELECT id FROM ${ENTITY_KEY_IDS.name} WHERE entity_set_id = ? $lowerboundSql ORDER BY id LIMIT 8000"
    }


    private fun getNextBatch(entitySetId: UUID, cursor: UUID, useLowerbound: Boolean): PostgresIterable<UUID> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val ps = connection.prepareStatement(getNextBatchQuery(entitySetId, useLowerbound))
                    ps.setObject(1, entitySetId)
                    if (useLowerbound) {
                        ps.setObject(2, cursor)
                    }
                    StatementHolder(connection, ps, ps.executeQuery())
                }, Function { ResultSetAdapters.id(it) }
        )

    }

    fun clearIndexingJobs(): Int {
        try {
            indexingLock.lock()
            val size = indexingJobs.size
            indexingQueue.clear()
            indexingJobs.clear()
            return size
        } finally {
            indexingLock.unlock()
        }
    }

    fun setForIndexing(entities: Map<UUID, Set<UUID>>) {
        try {
            indexingLock.lock()
            entities.forEach { entitySetId, entityKeyIds ->
                if (!indexingJobs.containsKey(entitySetId)) {
                    indexingQueue.put(entitySetId)
                }
                indexingJobs.set(entitySetId, DelegatedUUIDSet.wrap(entityKeyIds))
            }
        } finally {
            indexingLock.unlock()
        }

    }
}



