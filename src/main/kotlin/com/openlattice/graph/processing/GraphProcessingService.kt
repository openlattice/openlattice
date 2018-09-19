package com.openlattice.graph.processing

import com.google.common.collect.Multimaps
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.graph.processing.processors.GraphProcessor
import com.openlattice.graph.processing.processors.NoneProcessor
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.streams.PostgresIterable
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.locks.ReentrantLock

private const val EXPIRATION_MILLIS = 1000000L

class GraphProcessingService (
        private val edm: EdmManager,
        private val dqs: PostgresEntityDataQueryService,
        hazelcastInstance: HazelcastInstance,
        processorsToRegister: Set<GraphProcessor>
) {
    private val processors = mutableMapOf<UUID, GraphProcessor>().withDefault { NoneProcessor(dqs) }
    private val processingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.INDEXING_GRAPH_PROCESSING.name)

    companion object {
        private val logger = LoggerFactory.getLogger(GraphProcessingService.javaClass)
    }

    init {
        processorsToRegister.forEach { register(it) }
        processingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Scheduled(fixedRate = EXPIRATION_MILLIS)
    fun scavengeProcessingLocks() {
        processingLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    fun step() {
        if (taskLock.tryLock()) {
            try {
                val entityTypes = getAllowedEntityTypes()

                while (getActiveCount() > 0) {
                    entityTypes.forEach {
                        val lastPropagateTime = OffsetDateTime.now()

                        val lockedEntitySets = edm.getEntitySetsOfType(it).filter { tryLockEntitySet(it.id) }.map { it.id }
                        val activeEntities = getActiveEntities(lockedEntitySets)

                        //  entityset id / entity id / property id
                        val groupedEntities = activeEntities.groupBy({ it.entitySetId })

                        //TODO: get in right format
                        val entities = groupedEntities
                                .mapValues {
                                    dqs.getEntitiesById(it.key, edm.getPropertyTypesForEntitySet(it.key))
                                            .mapValues { Multimaps.asMap(it.value).mapValues { setOf(it.value) } }
                                }

                        processors[it]?.process(entities, lastPropagateTime)

                        lockedEntitySets.forEach(processingLocks::delete)
                    }
                }
            } finally {
                taskLock.unlock()
            }
        }
    }

    private fun getActiveEntities( entitySetIds : Collection<UUID>) : PostgresIterable<EntityDataKey> {
        return dqs.getActiveEntitiesById(entitySetIds)
    }

    private fun getActiveCount(): Long {
        val count = dqs.getActiveEntitiesCount()
        logger.info("Active entity count: $count.")
        return count
    }

    private fun getAllowedEntityTypes() : Set<UUID> {
        return edm.entityTypes.map{ it.id }.toSet()
    }

    private fun register( processor: GraphProcessor ) {
        processor.handledEntityTypes().forEach {
            processors[it] = processor
        }
    }

    private fun tryLockEntitySet(entitySetId: UUID): Boolean {
        return processingLocks.putIfAbsent(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS) == null
    }
}