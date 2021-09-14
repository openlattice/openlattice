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
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.util.PersonProperties
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.util.Optional
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EdmCachingDataLoader(
        private val dataQueryService: PostgresEntityDataQueryService,
        hazelcast: HazelcastInstance
) : DataLoader {
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap( hazelcast )

    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap( hazelcast )
    private val personEntityType = entityTypes.values(
            Predicates.equal(EntityTypeMapstore.FULLQUALIFIED_NAME_PREDICATE, PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString)
    ).first()
    private val authorizedPropertyTypesCache = Suppliers.memoizeWithExpiration(
            { propertyTypes.getAll(personEntityType.properties).filter{
                it.value.datatype != EdmPrimitiveTypeKind.Binary
            } },
            REFRESH_PROPERTY_TYPES_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS
    )

    private val linkingPropertyTypes = propertyTypes.getAll(personEntityType.properties).filter{
        it.value.datatype != EdmPrimitiveTypeKind.Binary && PersonProperties.FQNS.contains(it.value.type)
    }

    override fun getEntity(dataKey: EntityDataKey): Map<UUID, Set<Any>> {
        return getEntities(setOf(dataKey)).entries.first().value
    }

    override fun getEntities(dataKeys: Set<EntityDataKey>): Map<EntityDataKey, Map<UUID, Set<Any>>> {
        val entitiesByEDK = mutableMapOf<EntityDataKey, Map<UUID, Set<Any>>>()
        dataKeys.groupBy({ it.entitySetId }, { it.entityKeyId })
                .forEach { (entitySetId, entityKeyIds) ->
                    val entitiesById = getEntityStream(entitySetId, entityKeyIds.toSet())
                    entitiesByEDK.putAll(
                            entitiesById.associate {
                                EntityDataKey(entitySetId, it.first) to it.second
                            }
                    )
                }
        return entitiesByEDK
    }

    override fun getEntityStream(
            entitySetId: UUID, entityKeyIds: Set<UUID>
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>> {
        return dataQueryService.getEntitySetWithPropertyTypeIdsIterable(
                mapOf(entitySetId to Optional.of(entityKeyIds)),
                mapOf(entitySetId to authorizedPropertyTypesCache.get())
        )
    }

    override fun getLinkingEntity(dataKey: EntityDataKey): Map<UUID, Set<Any>> {
        val esid = dataKey.entitySetId
        return dataQueryService.getEntitySetWithPropertyTypeIdsIterable(
                mapOf(esid to Optional.of(setOf(dataKey.entityKeyId))),
                mapOf(esid to linkingPropertyTypes)
        ).first().second
    }

    override fun getLinkingEntities(dataKeys: Set<EntityDataKey>): Map<EntityDataKey, Map<UUID, Set<Any>>> {
        val entitiesByEDK = mutableMapOf<EntityDataKey, Map<UUID, Set<Any>>>()
        dataKeys.groupBy({ it.entitySetId }, { it.entityKeyId })
                .forEach { (entitySetId, entityKeyIds) ->
                    val entitiesById = getLinkingEntityStream(entitySetId, entityKeyIds.toSet())
                    entitiesByEDK.putAll(
                            entitiesById.associate {
                                EntityDataKey(entitySetId, it.first) to it.second
                            }
                    )
                }
        return entitiesByEDK
    }

    override fun getLinkingEntityStream(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>> {
        return dataQueryService.getEntitySetWithPropertyTypeIdsIterable(
                mapOf(entitySetId to Optional.of(entityKeyIds)),
                mapOf(entitySetId to linkingPropertyTypes)
        )
    }

}