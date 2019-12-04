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

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.admin.indexing.IndexingState
import com.openlattice.data.storage.getPartition
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.hazelcast.processors.UUIDKeyToUUIDSetMerger
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Stream

private val logger = LoggerFactory.getLogger(IndexingService::class.java)


private const val BATCH_LIMIT = 1_000
private val LB_UUID = UUID(0, 0)
private val IDS_WITH_LAST_WRITE = "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${IDS.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${PARTITION.name} = ? AND ${ID.name} = ANY(?) " +
        "ORDER BY ${ID.name} LIMIT $BATCH_LIMIT"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IndexingService(
        private val hds: HikariDataSource,
        private val backgroundIndexingService: BackgroundIndexingService,
        private val partitionManager: PartitionManager,
        executor: ListeningExecutorService,
        hazelcastInstance: HazelcastInstance
) {
    private val propertyTypes = hazelcastInstance.getMap<UUID, PropertyType>(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes = hazelcastInstance.getMap<UUID, EntityType>(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)

    private val indexingJobs = hazelcastInstance.getMap<UUID, DelegatedUUIDSet>(HazelcastMap.INDEXING_JOBS.name)
    private val indexingProgress = hazelcastInstance.getMap<UUID, UUID>(HazelcastMap.INDEXING_PROGRESS.name)
    private val indexingPartitionProgress = hazelcastInstance.getMap<UUID, Int>(
            HazelcastMap.INDEXING_PARTITION_PROGRESS.name
    )
    private val indexingPartitionList = hazelcastInstance.getMap<UUID, DelegatedIntList>(
            HazelcastMap.INDEXING_PARTITION_LIST.name
    )
    private val indexingQueue = hazelcastInstance.getQueue<UUID>(HazelcastQueue.INDEXING.name)
    private val indexingLock = ReentrantLock()

    init {
        //Add back in jobs that did not complete but are still registered.
        val jobIds = indexingJobs.keys
        jobIds.removeIf(indexingQueue::contains)
        logger.info("Re-adding the following jobs that were registered but not in the queue: {}", jobIds)
        if (jobIds.isNotEmpty()) {
            //If jobIds is empty it will add all entity sets for indexing.
            queueForIndexing( jobIds.map { it to emptySet<UUID>() }.toMap() )
        }
    }

    private val indexingWorker = executor.submit {
        Stream.generate { indexingQueue.take() }
                .parallel().forEach { entitySetId ->
                    try {
                        val entitySet = entitySets.getValue(entitySetId)
                        var cursor = indexingProgress.getOrPut(entitySetId) { LB_UUID }
                        val currentPartitions = partitionManager.getEntitySetPartitionsInfo(entitySetId)

                        val partitions = indexingPartitionList.getOrPut(entitySetId) {
                            DelegatedIntList(
                                    currentPartitions.partitions.toList()
                            )
                        }
                        val partitionCursor = indexingPartitionProgress.getOrPut(entitySetId) { 0 }

                        val propertyTypeMap = propertyTypes.getAll(
                                entityTypes.getValue(entitySet.entityTypeId).properties
                        )

                        val entityKeyIds: Set<UUID> = indexingJobs.getValue(entitySetId)

                        for (i in partitionCursor until partitions.size) {
                            var entityKeyIdsWithLastWrite: Map<UUID, OffsetDateTime> =
                                    StopWatch(
                                            "Loading index batch for entity ${entitySet.name} (${entitySet.id})} "
                                    ).use {

                                        //An empty set of ids means all keys
                                        if (entityKeyIds.isEmpty()) {
                                            getNextBatch(entitySetId, partitions[i], cursor).toMap()
                                        } else {
                                            getEntitiesWithLastWrite(entitySet.id, partitions, entityKeyIds).toMap()
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
                                cursor = entityKeyIdsWithLastWrite.keys.max()!!
                                indexingPartitionProgress.set(entitySetId, i)
                                indexingProgress.set(entitySetId, cursor)

                                StopWatch(
                                        "Loading index batch for entity ${entitySet.name} (${entitySet.id})} took "
                                ).use {

                                    entityKeyIdsWithLastWrite = getNextBatch(
                                            entitySetId,
                                            partitions[i],
                                            cursor
                                    ).toMap()
                                }
                            }
                        }

                        logger.info("Finished indexing entity set $entitySetId")
                        //We're done re-indexing this set.
                        try {
                            indexingLock.lock()
                            indexingJobs.delete(entitySetId)
                            indexingProgress.delete(entitySetId)
                            indexingPartitionList.delete(entitySetId)
                            indexingPartitionProgress.delete(entitySetId)
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

        entitiesForIndexing.forEach { (entitySetId, entityKeyIds) ->
            logger.info("Creating job to index entity set {} for the following entities {}", entitySetId, entityKeyIds)
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
                "WHERE ${ENTITY_SET_ID.name} = ? AND ${PARTITION.name} = ? AND ${VERSION.name} > 0 $lowerBoundSql " +
                "ORDER BY ${ID.name} LIMIT $BATCH_LIMIT"
    }

    private fun getNextBatch(
            entitySetId: UUID,
            partition: Int,
            cursor: UUID,
            useLowerBound: Boolean = (cursor !== LB_UUID)
    ): BasePostgresIterable<Pair<UUID, OffsetDateTime>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, getNextBatchQuery(useLowerBound)) { ps ->
            ps.setObject(1, entitySetId)
            ps.setInt(2, partition)
            if (useLowerBound) {
                ps.setObject(3, cursor)
            }
        }) { ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it) }
    }

    //TODO: This is unnecessary. If someone requests a re-index of a particular set of entity key ids, we should forcibly index those keys.
    private fun getEntitiesWithLastWrite(
            entitySetId: UUID,
            partitions: List<Int>,
            entityKeyIds: Set<UUID>
    ): Map<UUID, OffsetDateTime> {
        return entityKeyIds
                .groupBy { getPartition(it, partitions) }
                .flatMap { (partition, ids) -> getEntitiesWithLastWrite(entitySetId, partition, ids) }
                .toMap()
    }

    private fun getEntitiesWithLastWrite(
            entitySetId: UUID,
            partition: Int,
            entityKeyIds: Collection<UUID>
    ): BasePostgresIterable<Pair<UUID, OffsetDateTime>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, IDS_WITH_LAST_WRITE) { ps ->
            val idsArray = PostgresArrays.createUuidArray(ps.connection, entityKeyIds)
            ps.setObject(1, entitySetId)
            ps.setInt(2, partition)
            ps.setArray(3, idsArray)

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



