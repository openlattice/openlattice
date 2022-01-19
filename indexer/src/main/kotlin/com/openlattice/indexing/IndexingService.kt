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

import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.admin.indexing.IndexingState
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.hazelcast.processors.UUIDKeyToUUIDSetMerger
import com.openlattice.indexer.IndexerEntitySetMetadata
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.hazelcast.DelegatedUUIDSet
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.locks.ReentrantLock

private val logger = LoggerFactory.getLogger(IndexingService::class.java)


private const val BATCH_LIMIT = 1_000
private val LB_UUID = UUID(0, 0)
private val IDS_WITH_LAST_WRITE = "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${IDS.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID.name} = ANY(?) " +
        "ORDER BY ${ID.name} LIMIT $BATCH_LIMIT"

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
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)

    private val indexingJobs = HazelcastMap.INDEXING_JOBS.getMap(hazelcastInstance)
    private val indexingProgress = HazelcastMap.INDEXING_PROGRESS.getMap(hazelcastInstance)
    private val indexingQueue = HazelcastQueue.INDEXING.getQueue(hazelcastInstance)
    private val indexingLock = ReentrantLock()

    init {
        //Add back in jobs that did not complete but are still registered.
        val jobIds = indexingJobs.keys.filter { !indexingQueue.contains(it) }
        logger.info("Re-adding the following jobs that were registered but not in the queue: {}", jobIds)
        if (jobIds.isNotEmpty()) {
            //If jobIds is empty it will add all entity sets for indexing.
            queueForIndexing(jobIds.map { it to emptySet<UUID>() }.toMap())
        }
    }

    @Suppress("UNUSED")
    private val indexingWorker = executor.submit {
        while (true) {
            try {
                val entitySetId = indexingQueue.take()
                executor.submit {
                    val entitySet = IndexerEntitySetMetadata.fromEntitySet(entitySets.getValue(entitySetId))
                    var cursor = indexingProgress.getOrPut(entitySetId) { LB_UUID }

                    val propertyTypeMap = propertyTypes.getAll(
                            entityTypes.getValue(entitySet.entityTypeId).properties
                    )

                    val entityKeyIds: Set<UUID> = indexingJobs.getValue(entitySetId)

                    var entityKeyIdsWithLastWrite: Map<UUID, OffsetDateTime> =
                            StopWatch(
                                    "Loading index batch for entity ${entitySet.name} (${entitySet.id})} "
                            ).use {

                                //An empty set of ids means all keys
                                if (entityKeyIds.isEmpty()) {
                                    getNextBatch(entitySetId, cursor).toMap()
                                } else {
                                    getEntitiesWithLastWrite(entitySet.id, entityKeyIds).toMap()
                                }

                            }
                    while (entityKeyIdsWithLastWrite.isNotEmpty()) {
                        logger.info(
                                "Indexing entity set ${entitySet.name} (${entitySet.id}) starting at $cursor."
                        )
                        StopWatch(
                                "Indexing batch for entity ${entitySet.name} (${entitySet.id})} took "
                        ).use {
                            backgroundIndexingService.indexEntities(
                                    entitySet,
                                    entityKeyIdsWithLastWrite,
                                    propertyTypeMap,
                                    false
                            )
                        }
                        cursor = entityKeyIdsWithLastWrite.keys.maxOrNull()!!
                        indexingProgress.set(entitySetId, cursor)

                        StopWatch(
                                "Loading index batch for entity ${entitySet.name} (${entitySet.id})} took "
                        ).use {

                            entityKeyIdsWithLastWrite = getNextBatch(
                                    entitySetId,
                                    cursor
                            ).toMap()
                        }
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

        entitiesForIndexing.forEach { (entitySetId, entityKeyIds) ->
            logger.info("Creating job to index entity set $entitySetId for the following entities $entityKeyIds")
            indexingJobs.putIfAbsent(entitySetId, DelegatedUUIDSet.wrap(HashSet()))
            indexingJobs.executeOnKey(entitySetId, UUIDKeyToUUIDSetMerger(entityKeyIds))
            indexingQueue.put(entitySetId)
        }

        return entitiesForIndexing.size
    }

    private fun getNextBatchQuery(useLowerBound: Boolean): String {
        val lowerBoundSql = if (useLowerBound) {
            "AND id > ?"
        } else {
            ""
        }

        return "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = ? AND ${VERSION.name} > 0 $lowerBoundSql " +
                "ORDER BY ${ID.name} LIMIT $BATCH_LIMIT"
    }

    private fun getNextBatch(
            entitySetId: UUID,
            cursor: UUID,
            useLowerBound: Boolean = (cursor !== LB_UUID)
    ): BasePostgresIterable<Pair<UUID, OffsetDateTime>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getNextBatchQuery(useLowerBound)) { ps ->
            ps.setObject(1, entitySetId)
            if (useLowerBound) {
                ps.setObject(2, cursor)
            }
        }) { ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it) }
    }

    private fun getEntitiesWithLastWrite(
            entitySetId: UUID,
            entityKeyIds: Collection<UUID>
    ): BasePostgresIterable<Pair<UUID, OffsetDateTime>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, IDS_WITH_LAST_WRITE) { ps ->
            val idsArray = PostgresArrays.createUuidArray(ps.connection, entityKeyIds)
            ps.setObject(1, entitySetId)
            ps.setArray(2, idsArray)

        }) { ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it) }
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
            entities.forEach { (entitySetId, entityKeyIds) ->
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



