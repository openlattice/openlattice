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

import com.openlattice.data.DataGraphService
import com.openlattice.datastore.services.EdmManager
import java.util.*

/**
 *
 * This class makes it easy for other classes to implement auditing by passing a instance of the auditable event class
 * with the appropriate data configured.
 */
interface AuditingComponent {

    fun getAuditConfiguration(): AuditingConfiguration
    fun getDataGraphService(): DataGraphService
    fun getEdmManager() : EdmManager

    fun recordEvent(event: AuditableEvent): Int {
        return recordEvents(listOf(event))
    }

    fun recordEvents(events: List<AuditableEvent>): Int {
        val auditingConfiguration = getAuditConfiguration()
        val edm = getEdmManager()
        return events
                .groupBy { edm.getEntitySet(it.entitySet).activeAuditRecordEntitySetId }
                .map { (auditEntitySet, entities) ->
                    getDataGraphService().createEntities(
                            auditEntitySet,
                            toMap(entities),
                            auditingConfiguration.propertyTypes
                    ).size
                }.sum()

    }

    private fun toMap(events: List<AuditableEvent>): List<Map<UUID, Set<Any>>> {
        val auditingConfiguration = getAuditConfiguration()
        return events.map {
            val eventEntity = mutableMapOf<UUID, Set<Any>>()

            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.ENTITIES)] = it.entities.toSet()
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.PRINCIPAL)] = setOf(it.principal)
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.EVENT_TYPE)] = setOf(it.eventType)
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.DESCRIPTION)] = setOf(it.description)
            eventEntity[auditingConfiguration.getPropertyTypeId(AuditProperty.DATA)] = setOf(it.data)

            return@map eventEntity
        }
    }

}