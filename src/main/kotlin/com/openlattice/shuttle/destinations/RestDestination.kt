/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

package com.openlattice.shuttle.destinations

import com.geekbeast.util.ExponentialBackoff
import com.geekbeast.util.attempt
import com.openlattice.data.*
import com.openlattice.data.integration.Association
import com.openlattice.data.integration.Entity
import java.util.*

/**
 * Writes data using the REST API
 */

const val MAX_DELAY = 8L * 60L * 1000L

class RestDestination(
        private val dataApi: DataApi
) : IntegrationDestination {
    override fun integrateEntities(
            data: Collection<Entity>,
            entityKeyIds: Map<EntityKey, UUID>,
            updateTypes: Map<UUID, UpdateType>
    ): Long {
        val entitiesByEntitySet = data
                .groupBy({ it.entitySetId }, { entityKeyIds.getValue(it.key) to it.details })
                .mapValues { it.value.toMap() }

        return entitiesByEntitySet.entries.parallelStream().mapToLong { (entitySetId, entities) ->
            attempt(ExponentialBackoff(MAX_DELAY), MAX_RETRY_COUNT) {
                dataApi.updateEntitiesInEntitySet(entitySetId, entities, updateTypes[entitySetId]).toLong()
            }
        }.sum()
    }

    override fun integrateAssociations(
            data: Collection<Association>,
            entityKeyIds: Map<EntityKey, UUID>,
            updateTypes: Map<UUID, UpdateType>
    ): Long {
        val entities = data.map { Entity(it.key, it.details) }
        val edges = data.map {
            val srcDataKey = EntityDataKey(it.src.entitySetId, entityKeyIds[it.src])
            val dstDataKey = EntityDataKey(it.dst.entitySetId, entityKeyIds[it.dst])
            val edgeDataKey = EntityDataKey(it.key.entitySetId, entityKeyIds[it.key])
            DataEdgeKey(srcDataKey, dstDataKey, edgeDataKey)
        }.toSet()

        return integrateEntities(entities, entityKeyIds, updateTypes) +
                attempt(ExponentialBackoff(MAX_DELAY), MAX_RETRY_COUNT) {
                    dataApi.createEdges(edges).toLong()
                }
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.REST
    }
}