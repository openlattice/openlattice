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

package com.openlattice.linking.blocking

import com.google.common.collect.Multimaps
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicates
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.linking.Blocker
import com.openlattice.linking.DataLoader
import com.openlattice.linking.PERSON_FQN
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFuture
import java.util.*
import java.util.concurrent.Callable
import java.util.stream.Stream
import kotlin.streams.asSequence


private val logger = LoggerFactory.getLogger(ElasticsearchBlocker::class.java)

/**
 * Instantiates a blocking strategy that relies upon elasticsearch to compute initial blocks.
 */
@Component
class ElasticsearchBlocker(
        private val elasticsearch: ConductorElasticsearchApi,
        private val dataQueryService: PostgresEntityDataQueryService,
        private val dataLoader: DataLoader,
        hazelcast: HazelcastInstance
) : Blocker {

    private val entitySets: IMap<UUID, EntitySet> = hazelcast.getMap(HazelcastMap.ENTITY_SETS.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcast.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val propertyTypes: IMap<UUID, PropertyType> = hazelcast.getMap(HazelcastMap.PROPERTY_TYPES.name)

    private val personEntityType = entityTypes.values(
            Predicates.equal("type.fullQualifiedNameAsString", PERSON_FQN)
    ).first()

    override fun block(
            entityDataKey: EntityDataKey,
            entity: Optional<Map<UUID, Set<Any>>>,
            top: Int
    ): Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>> {
        val authorizedPropertyTypes = propertyTypes.getAll(personEntityType.properties)

        val blockedEntitySetSearchResults = elasticsearch.searchEntitySets(
                entitySets.values.filter { it.entityTypeId == personEntityType.id }.map(EntitySet::getId),
                getFieldSearches(entity.orElseGet { dataLoader.getEntity(entityDataKey) }),
                top,
                false
        )

        if (blockedEntitySetSearchResults[entityDataKey.entitySetId]?.contains(entityDataKey.entityKeyId) == false) {
            logger.error("Entity {} did not block to itself.", entityDataKey)
        }

        return entityDataKey to blockedEntitySetSearchResults
                .filter { it.value.isNotEmpty() }
                .entries
                .parallelStream()
                .flatMap { entry ->
                    dataLoader
                            .getEntityStream(entry.key, entry.value)
                            .stream()
                            .map { EntityDataKey(entry.key, it.first) to it.second }
                }
                .asSequence()
                .toMap()


    }

    private fun getEntity(
            entityDataKey: EntityDataKey, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<UUID, Set<Any>> {
        return Multimaps.asMap(
                dataQueryService.getEntitiesById(
                        entityDataKey.entitySetId,
                        authorizedPropertyTypes,
                        setOf(entityDataKey.entityKeyId)
                )[entityDataKey.entityKeyId]!!
        )
    }

    /**
     * Handles rendering an object into field searches for blocking.
     */
    private fun getFieldSearches(entity: Map<UUID, Set<Any>>): Map<UUID, DelegatedStringSet> {
        return entity.map { it.key to DelegatedStringSet.wrap(it.value.map { it.toString() }.toSet()) }.toMap()
    }
}