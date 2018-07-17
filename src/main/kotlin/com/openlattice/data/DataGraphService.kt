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

package com.openlattice.data

import com.codahale.metrics.annotation.Timed

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.collect.*
import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListenableFuture
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.analysis.requests.TopUtilizerDetails
import com.openlattice.data.analytics.IncrementableWeightId
import com.openlattice.data.integration.Association
import com.openlattice.data.integration.Entity
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.edge.EdgeKey
import com.openlattice.hazelcast.HazelcastMap
import org.apache.commons.collections4.keyvalue.MultiKey
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.stream.Stream
import kotlin.collections.HashMap

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val logger = LoggerFactory.getLogger(DataGraphService::class.java)

open class DataGraphService(
        hazelcastInstance: HazelcastInstance,
        private val eventBus: EventBus,
        private val lm: GraphService,
        private val idService: EntityKeyIdService,
        private val eds: EntityDatastore
) : DataGraphManager {
    //TODO: Move to a utility class
    companion object {
        @JvmStatic
        fun tryGetAndLogErrors(f: ListenableFuture<*>) {
            try {
                f.get()
            } catch (e: InterruptedException) {
                logger.error("Future execution failed.", e)
            } catch (e: ExecutionException) {
                logger.error("Future execution failed.", e)
            }
        }
    }

    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val typeIds: LoadingCache<UUID, UUID> = CacheBuilder.newBuilder()
            .maximumSize(100000) // 100K * 16 = 16000K = 16MB
            .build(
                    object : CacheLoader<UUID, UUID>() {

                        @Throws(Exception::class)
                        override fun load(key: UUID): UUID {
                            return entitySets[key]!!.entityTypeId
                        }
                    }
            )

    private val queryCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build<MultiKey<*>, Array<IncrementableWeightId>>()

    override fun getEntitySetData(
            entitySetId: UUID,
            orderedPropertyNames: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): EntitySetData<FullQualifiedName> {
        return eds.getEntitySetData(entitySetId, orderedPropertyNames, authorizedPropertyTypes)
    }


    override fun getEntitySetData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            orderedPropertyNames: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): EntitySetData<FullQualifiedName> {
        return eds.getEntities(entitySetId, entityKeyIds, orderedPropertyNames, authorizedPropertyTypes)
    }

    override fun deleteEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): Int {
        lm.deleteVerticesInEntitySet(entitySetId)
        return eds.deleteEntitySetData(entitySetId, authorizedPropertyTypes)
    }

    override fun getEntity(
            entitySetId: UUID,
            entityKeyId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): SetMultimap<FullQualifiedName, Any> {
        return eds
                .getEntities(entitySetId, ImmutableSet.of(entityKeyId), authorizedPropertyTypes)
                .iterator()
                .next()
    }

    override fun clearEntitySet(
            entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return 0
    }

    override fun clearEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return 0
    }

    override fun clearAssociations(key: Set<EdgeKey>): Int {
        return 0
    }

    //TODO: Return information about delete vertices.
    override fun deleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        lm.deleteVertices(entitySetId, entityKeyIds)
        return eds.deleteEntities(entitySetId, entityKeyIds, authorizedPropertyTypes)
    }

    @Timed
    override fun deleteAssociation(keys: Set<EdgeKey>, authorizedPropertyTypes: Map<UUID, PropertyType>): Int {
        val entitySetsToEntityKeyIds = HashMultimap.create<UUID, UUID>()

        keys.forEach {
            entitySetsToEntityKeyIds.put(it.edge.entitySetId, it.edge.entityKeyId)
        }

        return lm.deleteEdges(keys) + Multimaps.asMap(entitySetsToEntityKeyIds)
                .entries
                .stream()
                .mapToInt { e -> eds.deleteEntities(e.key, e.value, authorizedPropertyTypes) }
                .sum()
    }

    override fun integrateEntities(
            entitySetId: UUID,
            entities: Map<String, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<String, UUID> {
        //We need to fix this to avoid remapping. Skipping for expediency.
        return doIntegrateEntities(entitySetId, entities, authorizedPropertyTypes)
                .asSequence().map { it.key.entityId to it.value }.toMap()
    }

    private fun doIntegrateEntities(
            entitySetId: UUID,
            entities: Map<String, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<EntityKey, UUID> {
        val ids = idService.getEntityKeyIds(entities.keys.map { EntityKey(entitySetId, it) }.toSet())
        val identifiedEntities = ids.map { it.value to entities[it.key.entityId] }.toMap()
        eds.createOrUpdateEntities(entitySetId, identifiedEntities, authorizedPropertyTypes)
        return ids
    }

    override fun createEntities(
            entitySetId: UUID,
            entities: List<SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): List<UUID> {
        val ids = idService.reserveIds(entitySetId, entities.size)
        val entityMap = ids.mapIndexed({ i, id -> id to entities[i] }).toMap()
        eds.createOrUpdateEntities(entitySetId, entityMap, authorizedPropertyTypes)
        return ids
    }

    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return eds.replaceEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return eds.partialReplaceEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return eds.replacePropertiesInEntities(entitySetId, replacementProperties, authorizedPropertyTypes)
    }

    override fun createAssociations(
            associations: ListMultimap<UUID, DataEdge>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): ListMultimap<UUID, UUID> {
        val entityKeyIds: ListMultimap<UUID, UUID> = ArrayListMultimap.create()

        Multimaps
                .asMap(associations)
                .forEach {
                    val entitySetId = it.key
                    val entities = it.value.map { it.data }
                    val ids = createEntities(entitySetId, entities, authorizedPropertiesByEntitySetId[entitySetId]!!)

                    entityKeyIds.putAll(entitySetId, ids)

                    val edgeKeys = it.value.asSequence().mapIndexed { index, dataEdge ->
                        EdgeKey(dataEdge.src, dataEdge.dst, EntityDataKey(entitySetId, ids[index]))
                    }.toSet()
                    lm.createEdges(edgeKeys)
                }

        return entityKeyIds
    }

    override fun integrateAssociations(
            associations: Set<Association>,
            authorizedPropertiesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<String, UUID>> {
        val associationsByEntitySet = associations.groupBy { it.key.entitySetId }
        val entityKeys = HashSet<EntityKey>(3 * associations.size)
        val entityKeyIds = HashMap<EntityKey, UUID>(3 * associations.size)

        //Create the entities for the association and build list of required entity keys
        val integrationResults = associationsByEntitySet
                .asSequence()
                .map {
                    val entitySetId = it.key
                    val entities = it.value.asSequence()
                            .map {
                                entityKeys.add(it.src)
                                entityKeys.add(it.dst)
                                it.key.entityId to it.details
                            }.toMap()
                    val ids = doIntegrateEntities(entitySetId, entities, authorizedPropertiesByEntitySet[entitySetId]!!)
                    entityKeyIds.putAll(ids)
                    entitySetId to ids.asSequence().map { it.key.entityId to it.value }.toMap()
                }.toMap()
        //Retrieve the src/dst keys
        idService.getEntityKeyIds(entityKeys, entityKeyIds)

        //Create graph structure.
        val edges = associations
                .asSequence()
                .map { association ->
                    val srcId = entityKeyIds[association.src]
                    val dstId = entityKeyIds[association.dst]
                    val edgeId = entityKeyIds[association.key]

                    val src = EntityDataKey(association.src.entitySetId, srcId)
                    val dst = EntityDataKey(association.dst.entitySetId, dstId)
                    val edge = EntityDataKey(association.key.entitySetId, edgeId)

                    EdgeKey(src, dst, edge)
                }
                .toSet()
        lm.createEdges(edges)

        return integrationResults
    }

    override fun integrateEntitiesAndAssociations(
            entities: Set<Entity>,
            associations: Set<Association>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): IntegrationResults? {
        val entitiesByEntitySet = HashMap<UUID, MutableMap<String, SetMultimap<UUID, Any>>>()

        for (entity in entities) {
            val entitiesToCreate = entitiesByEntitySet.getOrPut(entity.entitySetId) { HashMap() }
            entitiesToCreate[entity.entityId] = entity.details
        }

        entitiesByEntitySet
                .forEach { entitySetId, entitySet ->
                    integrateEntities(
                            entitySetId,
                            entitySet,
                            authorizedPropertiesByEntitySetId[entitySetId]!!
                    )
                }

        integrateAssociations(associations, authorizedPropertiesByEntitySetId)
        return null
    }

    override fun getTopUtilizers(
            entitySetId: UUID,
            topUtilizerDetailsList: List<TopUtilizerDetails>,
            numResults: Int,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Stream<SetMultimap<FullQualifiedName, Any>> {
        /*
         * ByteBuffer queryId; try { queryId = ByteBuffer.wrap( ObjectMappers.getSmileMapper().writeValueAsBytes(
         * topUtilizerDetailsList ) ); } catch ( JsonProcessingException e1 ) { logger.debug(
         * "Unable to generate query id." ); return null; }
         */
        val maybeUtilizers = queryCache
                .getIfPresent(MultiKey(entitySetId, topUtilizerDetailsList))
        val utilizers: Array<IncrementableWeightId>
        // if ( !eds.queryAlreadyExecuted( queryId ) ) {
        if (maybeUtilizers == null) {
            //            utilizers = new TopUtilizers( numResults );
            val srcFilters = HashMultimap.create<UUID, UUID>()
            val dstFilters = HashMultimap.create<UUID, UUID>()

            topUtilizerDetailsList.forEach { details ->
                (if (details.utilizerIsSrc) srcFilters else dstFilters).putAll(
                        details.associationTypeId, details.neighborTypeIds
                )

            }
            utilizers = lm.computeGraphAggregation(numResults, entitySetId, srcFilters, dstFilters)

            queryCache.put(MultiKey(entitySetId, topUtilizerDetailsList), utilizers)
        } else {
            utilizers = maybeUtilizers
        }
        //TODO: this returns unsorted data.
        val utilizerIds = arrayOfNulls<UUID>(utilizers.size)
        for (i in utilizers.indices) {
            utilizerIds[i] = utilizers[i].id
        }
        return eds.getEntities(entitySetId, ImmutableSet.copyOf<UUID>(utilizerIds), authorizedPropertyTypes)
    }

    override fun getNeighborEntitySets(entitySetId: UUID): List<NeighborSets> {
        return lm.getNeighborEntitySets(entitySetId)
    }
}