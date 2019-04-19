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
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastQueue
import com.openlattice.hazelcast.processors.UUIDKeyToUUIDSetMerger
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.locks.ReentrantLock

private val logger = LoggerFactory.getLogger(IndexingService::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IndexingService(
        private val backgroundIndexingService: BackgroundIndexingService,
        private val executor: ListeningExecutorService,
        hazelcastInstance: HazelcastInstance
) {
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val indexingJobs = hazelcastInstance.getMap<UUID, DelegatedUUIDSet>(HazelcastMap.INDEXING_JOBS.name)
    private val indexingQueue = hazelcastInstance.getQueue<UUID>(HazelcastQueue.INDEXING.name)
    private val indexingLock = ReentrantLock()
    private val indexingWorker = executor.submit {
        while (true) {
            try {
                val entitySetId = indexingQueue.take()
                indexingLock.lock()
                val entitySet = entitySets.getValue(entitySetId)
                val entityKeyIds = Optional.ofNullable(indexingJobs[entitySetId])
                val propertyTypes = backgroundIndexingService.getPropertyTypeForEntityType(entitySet.entityTypeId)
                try {
                    entityKeyIds.ifPresentOrElse(
                            { entityKeyIds ->
                                backgroundIndexingService.indexEntities(
                                        entitySet,
                                        entityKeyIds,
                                        propertyTypes,
                                        false
                                )
                            },
                            {
                                backgroundIndexingService.indexEntitySet(
                                        entitySet,
                                        entityKeyIds.map { it.asIterable() })
                            })

                    indexingJobs.delete(entitySet.id)
                } catch (ex: Exception) {
                    logger.error("Error while indexing entity set ${entitySet.name} ($entitySetId)")
                }
            } finally {
                indexingLock.unlock()
            }
        }
    }

    fun getIndexingState(): IndexingState {
        return IndexingState(indexingJobs.getAll(indexingJobs.keys), indexingQueue.peek())
    }

    fun queueForIndexing(entities: Map<UUID, Set<UUID>>): Int {
        entities.forEach { entitySetId, entityKeyIds ->
            indexingJobs.executeOnKey(entitySetId, UUIDKeyToUUIDSetMerger(entityKeyIds))
            indexingQueue.put(entitySetId)
        }
        return 0
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



