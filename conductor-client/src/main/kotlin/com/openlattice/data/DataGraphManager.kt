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
import com.google.common.collect.ListMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking
import com.openlattice.analysis.requests.AggregationResult
import com.openlattice.analysis.requests.FilteredNeighborsRankingAggregation
import com.openlattice.data.storage.MetadataOption
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.NeighborSets
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.commons.lang3.tuple.Pair
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface DataGraphManager {

    /*
     * Entity set methods
     */
    fun getEntitySetData(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            orderedPropertyNames: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linking: Boolean
    ): EntitySetData<FullQualifiedName>

    fun getFilteredEntitySetData(
            entitySetId: UUID,
            filteredDataPageDefinition: FilteredDataPageDefinition,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<Map<FullQualifiedName, Set<Any>>>

    /*
     * CRUD methods for entity
     */
    fun getEntity(
            entitySetId: UUID,
            entityKeyId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<FullQualifiedName, Set<Any>>

    fun getLinkingEntity(
            entitySetIds: Set<UUID>,
            entityKeyId: UUID,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Map<FullQualifiedName, Set<Any>>

    fun getLinkedEntitySetBreakDown(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>>

    fun getEntitiesWithMetadata(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Iterable<MutableMap<FullQualifiedName, MutableSet<Any>>>

    /*
     * Bulk endpoints for entities/associations
     */

    fun getEntityKeyIds(entityKeys: Set<EntityKey>): Set<UUID>

    fun createEntities(
            entitySetId: UUID,
            entities: List<Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Pair<List<UUID>, WriteEvent>

    fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent

    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent

    fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent

    fun createAssociations(associations: Set<DataEdgeKey>): WriteEvent

    fun createAssociations(
            associations: ListMultimap<UUID, DataEdge>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, CreateAssociationEvent>

    fun getTopUtilizers(
            entitySetId: UUID,
            filteredNeighborsRankingList: List<FilteredNeighborsRankingAggregation>,
            numResults: Int,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Stream<SetMultimap<FullQualifiedName, Any>>

    fun getFilteredRankings(
            entitySetIds: Set<UUID>,
            numResults: Int,
            filteredRankings: List<AuthorizedFilteredNeighborsRanking>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linked: Boolean,
            linkingEntitySetId: Optional<UUID>
    ): AggregationResult

    fun getNeighborEntitySets(entitySetIds: Set<UUID>): List<NeighborSets>

    fun mergeEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent

    fun getNeighborEntitySetIds(entitySetIds: Set<UUID>): Set<UUID>

    /**
     * Returns all [DataEdgeKey]s that include requested entityKeyIds either as src, dst and/or edge with the requested
     * entity set id.
     * If includeClearedEdges is set to true, it will also return cleared (version < 0) entities.
     */
    fun getEdgesConnectedToEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, includeClearedEdges: Boolean
    ): BasePostgresIterable<DataEdgeKey>

    fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            currentDateTime: OffsetDateTime
    ): BasePostgresIterable<UUID>

    fun getEdgeEntitySetsConnectedToEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): Set<UUID>

    /**
     * Re-partitions the data for an entity set.
     *
     * NOTE: This function is a bit of a layer violation. It only migrates data from the provided partitions, which
     * must be known by the caller. It assumes that new partitions have been properly assigned to the entity set and
     * have been persisted to the database by the caller.
     *
     * @param entitySetId The id of the entity set to repartition
     * @param oldPartitions The previous data partitions for the entity set.
     */
    fun repartitionEntitySet(
            entitySetId: UUID,
            oldPartitions: Set<Int>,
            newPartitions: Set<Int>
    ): UUID

    /**
     * Deletes a set of edges from the graph.
     * @param associations Associations to delete
     */
    @Timed
    fun deleteAssociations(associations: Set<DataEdgeKey>, deleteType:DeleteType): WriteEvent
}