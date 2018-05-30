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
import com.openlattice.graph.core.Graph
import com.openlattice.graph.core.GraphApi
import com.openlattice.graph.core.objects.NeighborTripletSet
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

class DataGraphService(
        hazelcastInstance: HazelcastInstance,
        private val eventBus: EventBus,
        private val lm: GraphApi,
        private val idService: EntityKeyIdService,
        private val eds: EntityDatastore
) : DataGraphManager {
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

        keys.forEach() {
            entitySetsToEntityKeyIds.put(it.edgeEntitySetId, it.edgeEntityKeyId)
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
        val ids = idService.getEntityKeyIds(entities.keys.map { EntityKey(entitySetId, it) }.toSet())
        val identifiedEntities = ids.map { it.value to entities[it.key.entityId] }.toMap()
        eds.createEntities(entitySetId, identifiedEntities, authorizedPropertyTypes)
        //We need to fix this to avoid remapping. Skipping for expediency.
        return ids.map{ it.key.entityId to it.value }.toMap()
    }

    override fun createEntities(
            entitySetId: UUID,
            entities: List<SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): List<UUID> {
        val ids = idService.reserveIds(entitySetId, entities.size)
        val entityMap: MutableMap<UUID, SetMultimap<UUID, Any>> = HashMap()
        for (i in 0 until entities.size) {
            entityMap[ids[i]] = entities[i]
        }
        eds.createEntities(entitySetId, entityMap, authorizedPropertyTypes)
        return ids
    }

    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return 0
    }

    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return 0
    }

    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return 0
    }

    override fun createAssociations(
            associations: ListMultimap<UUID, DataEdge>?,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): ListMultimap<UUID, UUID> {
        val entityKeyIds: ListMultimap<UUID, UUID> = ArrayListMultimap.create()
        Multimaps
                .asMap(associations)
                .forEach {
                    val entitySetId = it.key
                    val ids = createEntities(
                            entitySetId, it.value.map { it.data }.toList(),
                            authorizedPropertiesByEntitySetId[entitySetId]!!
                    )
                    entityKeyIds.putAll(entitySetId, ids)

                    for (i in 0 until ids.size) {
                        lm.addEdge(
                                it.value[i].src.entityKeyId,
                                typeIds.getUnchecked(it.value[i].src.entitySetId),
                                it.value[i].src.entitySetId,
                                it.value[i].dst.entityKeyId,
                                typeIds.getUnchecked(it.value[i].dst.entitySetId),
                                it.value[i].dst.entitySetId,
                                ids[i],
                                typeIds.getUnchecked(entitySetId),
                                entitySetId
                        )
                    }
                }
        return entityKeyIds
    }

    override fun integrateAssociations(
            associations: Set<Association>,
            authorizedPropertiesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<String, UUID>>? {

        val associationEntitiesByEntitySet = HashMap<UUID, MutableMap<String, SetMultimap<UUID, Any>>>()
        val entityKeys = HashSet<EntityKey>(2 * associations.size)

        for (association in associations) {
            val associationEntities = associationEntitiesByEntitySet.getOrPut(
                    association.key.entitySetId
            ) { HashMap() }
            associationEntities[association.key.entityId] = association.details
            entityKeys.add(association.src)
            entityKeys.add(association.dst)
        }
        val entityKeyIds = idService.getEntityKeyIds(entityKeys)

        associationEntitiesByEntitySet
                .forEach { entitySetId, entities ->
                    integrateEntities(entitySetId, entities, authorizedPropertiesByEntitySet[entitySetId]!!)
                            .forEach { entityId, entityKeyId ->
                                entityKeyIds.put(
                                        EntityKey(entitySetId, entityId), entityKeyId
                                )
                            }
                }

        associations
                .parallelStream()
                .map { association ->
                    val edgeId = entityKeyIds[association.key]

                    val srcId = idService.getEntityKeyId(association.src)
                    val srcTypeId = typeIds.getUnchecked(association.src.entitySetId)
                    val srcSetId = association.src.entitySetId
                    val dstId = idService.getEntityKeyId(association.dst)
                    val dstTypeId = typeIds.getUnchecked(association.dst.entitySetId)
                    val dstSetId = association.dst.entitySetId
                    val edgeTypeId = typeIds.getUnchecked(association.key.entitySetId)
                    val edgeSetId = association.key.entitySetId

                    lm
                            .addEdgeAsync(
                                    srcId,
                                    srcTypeId,
                                    srcSetId,
                                    dstId,
                                    dstTypeId,
                                    dstSetId,
                                    edgeId,
                                    edgeTypeId,
                                    edgeSetId
                            )
                }.forEach { tryGetAndLogErrors(it) }
        return null
    }

    override fun integrateEntitiesAndAssociations(
            entities: Set<Entity>,
            associations: Set<Association>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): IntegrationResults? {
        // Map<EntityKey, UUID> idsRegistered = new HashMap<>();

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

    override fun getNeighborEntitySets(entitySetId: UUID): NeighborTripletSet {
        return lm.getNeighborEntitySets(entitySetId)
    }

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