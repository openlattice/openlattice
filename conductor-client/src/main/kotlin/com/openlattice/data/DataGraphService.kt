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
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.partioning.RepartitioningJob
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.commons.lang3.NotImplementedException
import org.apache.commons.lang3.tuple.Pair
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.sql.Types
import java.time.OffsetDateTime
import java.time.ZoneId
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

    companion object {
        const val ASSOCIATION_SIZE = 30_000
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
        ).iterator().next()
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

    override fun getEdgeKeysOfEntitySet(
            entitySetId: UUID, includeClearedEdges: Boolean
    ): BasePostgresIterable<DataEdgeKey> {
        return graphService.getEdgeKeysOfEntitySet(entitySetId, includeClearedEdges)
    }

    override fun getEdgesConnectedToEntities(entitySetId: UUID, entityKeyIds: Set<UUID>, includeClearedEdges: Boolean)
            : BasePostgresIterable<DataEdgeKey> {
        return graphService.getEdgeKeysContainingEntities(entitySetId, entityKeyIds, includeClearedEdges)
    }

    override fun getEdgeEntitySetsConnectedToEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): Set<UUID> {
        return graphService.getEdgeEntitySetsConnectedToEntities(entitySetId, entityKeyIds)
    }

    override fun getEdgeEntitySetsConnectedToEntitySet(entitySetId: UUID): Set<UUID> {
        return graphService.getEdgeEntitySetsConnectedToEntitySet(entitySetId)
    }

    override fun repartitionEntitySet(
            entitySetId: UUID,
            oldPartitions: Set<Int>,
            newPartitions: Set<Int>
    ): UUID {
        return jobService.submitJob(RepartitioningJob(entitySetId, oldPartitions.toList(), newPartitions))
    }

    /* Delete */

    private val groupEdges: (List<DataEdgeKey>) -> Map<UUID, Set<UUID>> = { edges ->
        edges.map { it.edge }.groupBy { it.entitySetId }.mapValues { it.value.map { it.entityKeyId }.toSet() }
    }

    override fun clearAssociationsBatch(
            entitySetId: UUID,
            associationsEdgeKeys: Iterable<DataEdgeKey>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<WriteEvent> {
        var associationClearCount = 0
        val writeEvents = ArrayList<WriteEvent>()

        associationsEdgeKeys.asSequence().chunked(ASSOCIATION_SIZE).forEach { dataEdgeKeys ->
            val entityKeyIds = groupEdges(dataEdgeKeys)
            entityKeyIds.entries.forEach {
                val writeEvent = clearEntityDataAndVerticesOfAssociations(
                        dataEdgeKeys, it.key, it.value, authorizedPropertyTypes.getValue(it.key)
                )
                writeEvents.add(writeEvent)
                associationClearCount += writeEvent.numUpdates
            }
        }

        logger.info(
                "Cleared {} associations when deleting entities from entity set {}", associationClearCount,
                entitySetId
        )

        return writeEvents
    }

    private fun clearEntityDataAndVerticesOfAssociations(
            dataEdgeKeys: List<DataEdgeKey>,
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // clear edges
        val verticesCount = graphService.clearEdges(dataEdgeKeys)

        //clear entities
        val entityWriteEvent = eds.clearEntities(entitySetId, entityKeyIds, authorizedPropertyTypes)

        logger.info("Cleared {} entities and {} vertices.", entityWriteEvent.numUpdates, verticesCount)
        return entityWriteEvent
    }

    override fun deleteAssociationsBatch(
            entitySetId: UUID,
            associationsEdgeKeys: Iterable<DataEdgeKey>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<WriteEvent> {
        var associationDeleteCount = 0
        val writeEvents = ArrayList<WriteEvent>()

        associationsEdgeKeys.asSequence().chunked(ASSOCIATION_SIZE).forEach { dataEdgeKeys ->
            val entityKeyIds = groupEdges(dataEdgeKeys)
            entityKeyIds.entries.forEach {
                val writeEvent = deleteEntityDataAndVerticesOfAssociations(
                        dataEdgeKeys, it.key, it.value, authorizedPropertyTypes.getValue(it.key)
                )
                writeEvents.add(writeEvent)
                associationDeleteCount += writeEvent.numUpdates
            }
        }

        logger.info(
                "Deleted {} associations when deleting entities from entity set {}", associationDeleteCount,
                entitySetId
        )

        return writeEvents
    }

    private fun deleteEntityDataAndVerticesOfAssociations(
            dataEdgeKeys: List<DataEdgeKey>,
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // delete edges
        val verticesCount = graphService.deleteEdges(dataEdgeKeys)

        // delete entities
        val entityWriteEvent = eds.deleteEntities(entitySetId, entityKeyIds, authorizedPropertyTypes)

        logger.info("Deleted {} entities and {} vertices.", entityWriteEvent.numUpdates, verticesCount.numUpdates)

        return entityWriteEvent
    }

    /* Create */

    override fun createEntities(
            entitySetId: UUID,
            entities: List<Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Pair<List<UUID>, WriteEvent> {
        val ids = idService.reserveIds(entitySetId, entities.size)
        val entityMap = ids.mapIndexed { i, id -> id to entities[i] }.toMap()
        val writeEvent = eds.createOrUpdateEntities(entitySetId, entityMap, authorizedPropertyTypes)

        return Pair.of(ids, writeEvent)
    }

    override fun mergeEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return eds.createOrUpdateEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return eds.replaceEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return eds.partialReplaceEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return eds.replacePropertiesInEntities(entitySetId, replacementProperties, authorizedPropertyTypes)
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
                            entitySetId, entities, authorizedPropertiesByEntitySetId.getValue(entitySetId)
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
            entitySetId: UUID, expirationPolicy: DataExpiration,
            dateTime: OffsetDateTime, deleteType: DeleteType,
            expirationPropertyType: Optional<PropertyType>
    ): BasePostgresIterable<UUID> {
        val sqlParams = getSqlParameters(expirationPolicy, dateTime, expirationPropertyType)
        val expirationBaseColumn = sqlParams.first
        val formattedDateMinusTTE = sqlParams.second
        val sqlFormat = sqlParams.third
        return eds.getExpiringEntitiesFromEntitySet(
                entitySetId, expirationBaseColumn, formattedDateMinusTTE,
                sqlFormat, deleteType
        )
    }

    private fun getSqlParameters(
            expirationPolicy: DataExpiration, dateTime: OffsetDateTime, expirationPT: Optional<PropertyType>
    ): Triple<String, Any, Int> {
        val expirationBaseColumn: String
        val formattedDateMinusTTE: Any
        val sqlFormat: Int
        val dateMinusTTEAsInstant = dateTime.toInstant().minusMillis(expirationPolicy.timeToExpiration)
        when (expirationPolicy.expirationBase) {
            ExpirationBase.DATE_PROPERTY -> {
                val expirationPropertyType = expirationPT.get()
                val columnData = Pair(
                        expirationPropertyType.postgresIndexType,
                        expirationPropertyType.datatype
                )
                expirationBaseColumn = PostgresDataTables.getColumnDefinition(columnData.first, columnData.second).name
                if (columnData.second == EdmPrimitiveTypeKind.Date) {
                    formattedDateMinusTTE = OffsetDateTime.ofInstant(
                            dateMinusTTEAsInstant, ZoneId.systemDefault()
                    ).toLocalDate()
                    sqlFormat = Types.DATE
                } else { //only other TypeKind for date property type is OffsetDateTime
                    formattedDateMinusTTE = OffsetDateTime.ofInstant(dateMinusTTEAsInstant, ZoneId.systemDefault())
                    sqlFormat = Types.TIMESTAMP_WITH_TIMEZONE
                }
            }
            ExpirationBase.FIRST_WRITE -> {
                expirationBaseColumn = "${PostgresColumn.VERSIONS.name}[array_upper(${PostgresColumn.VERSIONS.name},1)]" //gets the latest version from the versions column
                formattedDateMinusTTE = dateMinusTTEAsInstant.toEpochMilli()
                sqlFormat = Types.BIGINT
            }
            ExpirationBase.LAST_WRITE -> {
                expirationBaseColumn = DataTables.LAST_WRITE.name
                formattedDateMinusTTE = OffsetDateTime.ofInstant(dateMinusTTEAsInstant, ZoneId.systemDefault())
                sqlFormat = Types.TIMESTAMP_WITH_TIMEZONE
            }
        }
        return Triple(expirationBaseColumn, formattedDateMinusTTE, sqlFormat)
    }
}