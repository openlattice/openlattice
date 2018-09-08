/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.linking

import com.google.common.base.Suppliers
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicates
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.streams.PostgresIterable
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EdmCachingDataLoader(
        private val dataQueryService: PostgresEntityDataQueryService,
        hazelcast: HazelcastInstance
) : DataLoader {
    companion object {
        private val logger = LoggerFactory.getLogger(EdmCachingDataLoader::class.java)
    }

    private val entityTypes: IMap<UUID, EntityType> = hazelcast.getMap(HazelcastMap.ENTITY_TYPES.name)

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcast.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val personEntityType = entityTypes.values(
            Predicates.equal("type.fullQualifiedNameAsString", PERSON_FQN)
    ).first()
    private val authorizedPropertyTypesCache = Suppliers.memoizeWithExpiration(
            { propertyTypes.getAll(personEntityType.properties) },
            REFRESH_PROPERTY_TYPES_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS
    )

    override fun getEntity(dataKey: EntityDataKey): Map<UUID, Set<Any>> {
        return getEntities(setOf(dataKey)).entries.first().value
    }

    override fun getEntities(dataKeys: Set<EntityDataKey>): Map<EntityDataKey, Map<UUID, Set<Any>>> {
        return dataKeys
                .groupBy({ it.entitySetId }, { it.entityKeyId })
                .mapValues { it.value.toSet() }
                .flatMap { edkp ->
                    getEntityStream(edkp.key, edkp.value).map { EntityDataKey(edkp.key, it.first) to it.second }
                }
                .toMap()
    }

    override fun getEntityStream(
            entitySetId: UUID, entityKeyIds: Set<UUID>
    ): PostgresIterable<Pair<UUID, Map<UUID, Set<Any>>>> {
        return dataQueryService.streamableEntitySetWithEntityKeyIdsAndPropertyTypeIds(
                entitySetId,
                Optional.of(entityKeyIds),
                authorizedPropertyTypesCache.get(),
                EnumSet.noneOf(MetadataOption::class.java)
        )
    }

}