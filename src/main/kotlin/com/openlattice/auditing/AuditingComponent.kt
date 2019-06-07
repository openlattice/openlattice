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

package com.openlattice.auditing

import com.dataloom.mappers.ObjectMappers
import com.google.common.collect.*
import com.openlattice.data.DataEdge
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityDataKey
import java.util.*

private val mapper = ObjectMappers.newJsonMapper()

/**
 *
 * This class makes it easy for other classes to implement auditing by passing a instance of the auditable event class
 * with the appropriate data configured.
 */

interface AuditingComponent {

    companion object {
        const val MAX_ENTITY_KEY_IDS_PER_EVENT = 100
    }

    fun getAuditRecordEntitySetsManager(): AuditRecordEntitySetsManager
    fun getDataGraphService(): DataGraphManager

    @JvmDefault
    fun recordEvent(event: AuditableEvent): Int {
        return recordEvents(listOf(event))
    }

    @JvmDefault
    fun recordEvents(events: List<AuditableEvent>): Int {

        val ares = getAuditRecordEntitySetsManager()
        val auditingConfiguration = ares.auditingTypes

        return if (auditingConfiguration.isAuditingInitialized()) {
            events
                    .groupBy { ares.getActiveAuditEntitySetIds(it.aclKey, it.eventType) }
                    .filter { (auditEntitySetConfiguration, entities) -> auditEntitySetConfiguration.auditRecordEntitySet != null }
                    .map { (auditEntitySetConfiguration, entities) ->
                        val auditEntitySet = auditEntitySetConfiguration.auditRecordEntitySet
                        val (entityKeyIds, _) = getDataGraphService().createEntities(
                                auditEntitySet!!,
                                toMap(entities),
                                auditingConfiguration.propertyTypes
                        )

                        val lm = ArrayListMultimap.create<UUID, DataEdge>()
                        entityKeyIds.asSequence().zip(entities.asSequence())
                                .filter { it.second.entities.isPresent }
                                .forEach { (auditEntityKeyId, ae) ->
                                    val entitySetId = ae.aclKey[0]
                                    val entityKeyIds = ae.entities.get()
                                    entityKeyIds.forEach { id ->
                                        lm.put(
                                                auditEntitySetConfiguration.auditEdgeEntitySet,
                                                DataEdge(
                                                        EntityDataKey(entitySetId, id),
                                                        EntityDataKey(auditEntitySet, auditEntityKeyId),
                                                        ImmutableMap.of(auditEntitySet, emptySet())
                                                )
                                        )
                                        return@forEach
                                    }
                                }
                        getDataGraphService().createAssociations(lm, ImmutableMap.of())
                        entityKeyIds.size
                    }.sum()
        } else {
            0
        }
    }

    private fun toMap(events: List<AuditableEvent>): List<Map<UUID, Set<Any>>> {
        val auditingConfiguration = getAuditRecordEntitySetsManager().auditingTypes
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