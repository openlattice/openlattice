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
import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.google.common.base.Stopwatch
import com.google.common.collect.ListMultimap
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking
import com.openlattice.analysis.requests.AggregationResult
import com.openlattice.analysis.requests.FilteredNeighborsRankingAggregation
import com.openlattice.data.storage.EntityDatastore
import com.openlattice.data.storage.MetadataOption
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.partioning.RepartitioningJob
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.commons.lang3.NotImplementedException
import org.apache.commons.lang3.tuple.Pair
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val logger = LoggerFactory.getLogger(DataGraphService::class.java)

@Service
class DataGraphService(
        private val graphService: GraphService,
        private val idService: EntityKeyIdService,
        private val eds: EntityDatastore,
        private val jobService: HazelcastJobService
) : DataGraphManager {
    override fun getEntitiesWithMetadata(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Iterable<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        throw NotImplementedException("NOT YET IMPLEMENTED.")
    }

    override fun getEntityKeyIds(entityKeys: Set<EntityKey>): Set<UUID> {
        return idService.reserveEntityKeyIds(entityKeys)
    }

    /* Select */

    override fun getEntitySetData(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            orderedPropertyNames: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linking: Boolean
    ): EntitySetData<FullQualifiedName> {
        return eds.getEntities(entityKeyIds, orderedPropertyNames, authorizedPropertyTypes, linking)
    }

    override fun getFilteredEntitySetData(
            entitySetId: UUID,
            filteredDataPageDefinition: FilteredDataPageDefinition,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<Map<FullQualifiedName, Set<Any>>> {
        return eds.getFilteredEntitySetData(entitySetId, filteredDataPageDefinition, authorizedPropertyTypes)
    }

    override fun getEntity(
            entitySetId: UUID,
            entityKeyId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<FullQualifiedName, Set<Any>> {
        return eds.getEntities(
                entitySetId,
                setOf(entityKeyId),
                mapOf(entitySetId to authorizedPropertyTypes)
        ).iterator().next()
    }

    override fun getLinkingEntity(
            entitySetIds: Set<UUID>,
            entityKeyId: UUID,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Map<FullQualifiedName, Set<Any>> {
        return eds.getLinkingEntities(
                entitySetIds.map { it to Optional.of(setOf(entityKeyId)) }.toMap(),
                authorizedPropertyTypes
        ).first()
    }

    override fun getLinkedEntitySetBreakDown(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>> {
        return eds.getLinkedEntitySetBreakDown(linkingIdsByEntitySetId, authorizedPropertyTypesByEntitySetId)
    }

    override fun getNeighborEntitySets(entitySetIds: Set<UUID>): List<NeighborSets> {
        return graphService.getNeighborEntitySets(entitySetIds)
    }

    override fun getNeighborEntitySetIds(entitySetIds: Set<UUID>): Set<UUID> {
        return getNeighborEntitySets(entitySetIds)
                .flatMap { listOf(it.srcEntitySetId, it.edgeEntitySetId, it.dstEntitySetId) }
                .toSet()
    }

    override fun getEdgesConnectedToEntities(entitySetId: UUID, entityKeyIds: Set<UUID>, includeClearedEdges: Boolean)
            : BasePostgresIterable<DataEdgeKey> {
        return graphService.getEdgeKeysContainingEntities(entitySetId, entityKeyIds, includeClearedEdges)
    }

    override fun getEdgeEntitySetsConnectedToEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): Set<UUID> {
        return graphService.getEdgeEntitySetsConnectedToEntities(entitySetId, entityKeyIds)
    }

    override fun repartitionEntitySet(
            entitySetId: UUID,
            oldPartitions: Set<Int>,
            newPartitions: Set<Int>
    ): UUID {
        return jobService.submitJob(RepartitioningJob(entitySetId, oldPartitions.toList(), newPartitions))
    }

    /* Create */

    override fun createEntities(
            entitySetId: UUID,
            entities: List<Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Pair<List<UUID>, WriteEvent> {
        val ids = idService.reserveIds(entitySetId, entities.size)
        val entityMap = ids.zip(entities).toMap()
        val writeEvent = eds.createOrUpdateEntities(
                entitySetId,
                entityMap,
                authorizedPropertyTypes,
                PropertyUpdateType.Versioned
        )

        return Pair.of(ids, writeEvent)
    }

    override fun mergeEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        return eds.createOrUpdateEntities(entitySetId, entities, authorizedPropertyTypes, propertyUpdateType)
    }

    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        return eds.replaceEntities(entitySetId, entities, authorizedPropertyTypes, propertyUpdateType)
    }

    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        return eds.partialReplaceEntities(entitySetId, entities, authorizedPropertyTypes, propertyUpdateType)
    }

    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        return eds.replacePropertiesInEntities(
                entitySetId,
                replacementProperties,
                authorizedPropertyTypes,
                propertyUpdateType
        )
    }

    override fun createAssociations(associations: Set<DataEdgeKey>): WriteEvent {
        return graphService.createEdges(associations)
    }

    override fun createAssociations(
            associations: ListMultimap<UUID, DataEdge>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, CreateAssociationEvent> {

        val associationCreateEvents: MutableMap<UUID, CreateAssociationEvent> = mutableMapOf()

        Multimaps
                .asMap(associations)
                .forEach {
                    val entitySetId = it.key

                    val entities = it.value.map(DataEdge::getData)
                    val (ids, entityWrite) = createEntities(
                            entitySetId,
                            entities,
                            authorizedPropertiesByEntitySetId.getValue(entitySetId)
                    )

                    val edgeKeys = it.value.asSequence().mapIndexed { index, dataEdge ->
                        DataEdgeKey(dataEdge.src, dataEdge.dst, EntityDataKey(entitySetId, ids[index]))
                    }.toSet()
                    val sw = Stopwatch.createStarted()
                    val edgeWrite = graphService.createEdges(edgeKeys)
                    logger.info(
                            "graphService.createEdges (for {} edgeKeys) took {}", edgeKeys.size,
                            sw.elapsed(TimeUnit.MILLISECONDS)
                    )

                    associationCreateEvents[entitySetId] = CreateAssociationEvent(ids, entityWrite, edgeWrite)
                }

        return associationCreateEvents
    }

    @Timed
    override fun deleteAssociations(associations: Set<DataEdgeKey>, deleteType: DeleteType): WriteEvent {
        return graphService.deleteEdges(associations, deleteType)
    }

    /* Top utilizers */
    @Timed
    override fun getFilteredRankings(
            entitySetIds: Set<UUID>,
            numResults: Int,
            filteredRankings: List<AuthorizedFilteredNeighborsRanking>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linked: Boolean,
            linkingEntitySetId: Optional<UUID>
    ): AggregationResult {
        return graphService.computeTopEntities(
                numResults,
                entitySetIds,
                authorizedPropertyTypes,
                filteredRankings,
                linked,
                linkingEntitySetId
        )

    }

    override fun getTopUtilizers(
            entitySetId: UUID,
            filteredNeighborsRankingList: List<FilteredNeighborsRankingAggregation>,
            numResults: Int,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Stream<SetMultimap<FullQualifiedName, Any>> {
        return Stream.empty()
    }

    override fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            currentDateTime: OffsetDateTime
    ): BasePostgresIterable<UUID> {
        return eds.getExpiringEntitiesFromEntitySet(entitySetId, expirationPolicy, currentDateTime)
    }

}