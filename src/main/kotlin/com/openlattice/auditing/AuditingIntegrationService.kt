package com.openlattice.auditing

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ImmutableMap
import com.google.common.util.concurrent.MoreExecutors
import com.hazelcast.core.HazelcastInstance
import com.openlattice.data.DataEdge
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityDataKey
import com.openlattice.hazelcast.HazelcastQueue
import java.util.*
import java.util.concurrent.Executors
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AuditingIntegrationService(
        hazelcastInstance: HazelcastInstance,
        private val ares: AuditRecordEntitySetsManager,
        private val dgm: DataGraphManager,
        private val s3AuditingService: S3AuditingService,
        private val mapper: ObjectMapper
) {
    val auditingQueue = hazelcastInstance.getQueue<AuditableEvent>(HazelcastQueue.AUDITING.name)
    val integrationExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    val loadingExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())

    val loadingWorker = loadingExecutor.execute {

        while (true) {
            var events = s3AuditingService.getRecordedEvents()
            events.forEach(auditingQueue::put)
            while (events.isNotEmpty()) {
                events = s3AuditingService.getRecordedEvents()
                events.forEach(auditingQueue::put)
            }
        }
    }
    val integrationWorker = integrationExecutor.execute {
        Stream.generate { auditingQueue.take() }
                .parallel()
                .forEach { integrateEvent(it) }
    }

    private fun integrateEvent(event: AuditableEvent): Int {
        val auditingConfiguration = ares.auditingTypes

        return if (auditingConfiguration.isAuditingInitialized()) {
            //TODO: Fix having to wrap in a list.
            mapOf(ares.getActiveAuditEntitySetIds(event.aclKey, event.eventType) to listOf(event))
                    .filter { (auditEntitySetConfiguration, _) ->
                        auditEntitySetConfiguration.auditRecordEntitySet != null
                    }
                    .map { (auditEntitySetConfiguration, entities) ->
                        val auditEntitySet = auditEntitySetConfiguration.auditRecordEntitySet
                        val (entityKeyIds, _) = dgm.createEntities(
                                auditEntitySet!!,
                                toMap(entities),
                                auditingConfiguration.propertyTypes
                        )

                        if (auditEntitySetConfiguration.auditEdgeEntitySet != null) {
                            val auditEdgeEntitySet = auditEntitySetConfiguration.auditEdgeEntitySet

                            val lm = ArrayListMultimap.create<UUID, DataEdge>()
                            entityKeyIds.asSequence().zip(entities.asSequence())
                                    .filter { it.second.entities.isPresent }
                                    .forEach { (auditEntityKeyId, ae) ->
                                        val aeEntitySetId = ae.aclKey[0]
                                        val aeEntityKeyIds = ae.entities.get()
                                        aeEntityKeyIds.forEach { id ->
                                            lm.put(
                                                    auditEdgeEntitySet,
                                                    DataEdge(
                                                            EntityDataKey(aeEntitySetId, id),
                                                            EntityDataKey(auditEntitySet, auditEntityKeyId),
                                                            ImmutableMap.of()
                                                    )
                                            )
                                            return@forEach
                                        }
                                    }
                            dgm.createAssociations(lm, ImmutableMap.of(auditEdgeEntitySet, emptyMap()))

                        }
                        entityKeyIds.size
                    }.sum()
        } else {
            0
        }
    }

    private fun toMap(events: List<AuditableEvent>): List<Map<UUID, Set<Any>>> {
        val auditingConfiguration = ares.auditingTypes
        return events.map { event ->
            val eventEntity = mutableMapOf<UUID, Set<Any>>()

            eventEntity[auditingConfiguration.getPropertyTypeId(
                    AuditProperty.ACL_KEY
            )] = setOf(event.aclKey.index)

            event.entities.ifPresent {
                eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.ENTITIES)] = it
            }

            event.operationId.ifPresent {
                eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.OPERATION_ID)] = setOf(it)
            }

            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.ID)] = setOf(
                    event.aclKey.last().toString()
            ) //ID of securable object
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.PRINCIPAL)] = setOf(
                    event.principal.toString()
            )
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.EVENT_TYPE)] = setOf(event.eventType.name)
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.DESCRIPTION)] = setOf(event.description)
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.DATA)] = setOf(
                    mapper.writeValueAsString(event.data)
            )
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.TIMESTAMP)] = setOf(event.timestamp)

            return@map eventEntity
        }
    }
}
