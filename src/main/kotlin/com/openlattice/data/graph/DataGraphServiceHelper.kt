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
package com.openlattice.data.graph

import com.google.common.collect.ListMultimap
import com.openlattice.data.DataEdge
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.integration.Association
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.type.AssociationType
import java.util.UUID

class DataGraphServiceHelper(private val entitySetManager: EntitySetManager) {

    /**
     * Checks whether the entity type of the src and dst entity sets in each edge are part of allowed src and dst
     * entity types of the association entity type.
     */
    fun checkEdgeEntityTypes(edges: Set<DataEdgeKey>) {
        //Create graph structure and check entity types
        val srcAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-src
        val dstAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-dst

        val edgeEsIdFqn = { edge: Any -> (edge as DataEdgeKey).edge.entitySetId }
        val srcEsIdFqn = { edge: Any -> (edge as DataEdgeKey).src.entitySetId }
        val dstEsIdFqn = { edge: Any -> (edge as DataEdgeKey).dst.entitySetId }

        collectEntitySetIds(
                srcAssociationEntitySetIds, dstAssociationEntitySetIds, edges, edgeEsIdFqn, srcEsIdFqn, dstEsIdFqn
        )

        checkAssociationEntityTypes(srcAssociationEntitySetIds, dstAssociationEntitySetIds)
    }

    /**
     * Checks whether the entity type of the src and dst entity sets in each association are part of allowed src and dst
     * entity types of the association entity type.
     */
    fun checkAssociationEntityTypes(associations: Set<Association>) {
        //Create graph structure and check entity types
        val srcAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-src
        val dstAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-dst


        val edgeEsIdFqn = { association: Any -> (association as Association).key.entitySetId }
        val srcEsIdFqn = { association: Any -> (association as Association).src.entitySetId }
        val dstEsIdFqn = { association: Any -> (association as Association).dst.entitySetId }

        collectEntitySetIds(
                srcAssociationEntitySetIds,
                dstAssociationEntitySetIds,
                associations,
                edgeEsIdFqn,
                srcEsIdFqn,
                dstEsIdFqn
        )

        checkAssociationEntityTypes(srcAssociationEntitySetIds, dstAssociationEntitySetIds)
    }

    /**
     * Checks whether the entity type of the src and dst entity sets in each association are part of allowed src and dst
     * entity types of the association entity type.
     */
    fun checkAssociationEntityTypes(associations: ListMultimap<UUID, DataEdge>) {
        //Create graph structure and check entity types
        val srcAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-src
        val dstAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-dst

        val srcEsIdFqn = { association: Any -> (association as DataEdge).src.entitySetId }
        val dstEsIdFqn = { association: Any -> (association as DataEdge).dst.entitySetId }

        associations.asMap().forEach { (edgeEsId, edges) ->
            val edgeEsIdFqn = { _: Any -> edgeEsId }

            collectEntitySetIds(
                    srcAssociationEntitySetIds, dstAssociationEntitySetIds, edges, edgeEsIdFqn, srcEsIdFqn, dstEsIdFqn
            )
        }

        // ensure, that src and dst entity types are part of src and dst entity types of AssociationType
        checkAssociationEntityTypes(srcAssociationEntitySetIds, dstAssociationEntitySetIds)
    }

    private fun collectEntitySetIds(
            srcAssociationEntitySetIds: MutableMap<UUID, MutableSet<UUID>>,
            dstAssociationEntitySetIds: MutableMap<UUID, MutableSet<UUID>>,
            associations: Collection<Any>,
            edgeEsIdFqn: (Any) -> UUID,
            srcEsIdFqn: (Any) -> UUID,
            dstEsIdFqn: (Any) -> UUID
    ) {
        associations.forEach { association ->
            val edgeEsId = edgeEsIdFqn(association)
            val srcEsId = srcEsIdFqn(association)
            val dstEsId = dstEsIdFqn(association)

            if (srcAssociationEntitySetIds.putIfAbsent(edgeEsId, mutableSetOf(srcEsId)) != null) {
                srcAssociationEntitySetIds.getValue(edgeEsId).add(srcEsId)
            }

            if (dstAssociationEntitySetIds.putIfAbsent(edgeEsId, mutableSetOf(dstEsId)) != null) {
                dstAssociationEntitySetIds.getValue(edgeEsId).add(dstEsId)
            }
        }
    }

    /**
     * Checks entity types of associations against the allowed src and dst entity types in association type.
     * @param srcAssociationEntitySetIds The entity set ids of the src entities in associations mapped by their
     * association entity set id.
     * @param dstAssociationEntitySetIds The entity set ids of the dst entities in associations mapped by their
     * association entity set id.
     */
    private fun checkAssociationEntityTypes(
            srcAssociationEntitySetIds: Map<UUID, Set<UUID>>,
            dstAssociationEntitySetIds: Map<UUID, Set<UUID>>
    ) {
        val associationTypes = entitySetManager.getAssociationTypeDetailsByEntitySetIds(srcAssociationEntitySetIds.keys)
        val allSrcEntityTypes = entitySetManager
                .getEntityTypeIdsByEntitySetIds(srcAssociationEntitySetIds.values.flatten().toSet())
        val allDstEntityTypes = entitySetManager
                .getEntityTypeIdsByEntitySetIds(dstAssociationEntitySetIds.values.flatten().toSet())

        associationTypes.keys.forEach { edgeEntitySetId ->
            val srcEntitySetIds = srcAssociationEntitySetIds.getValue(edgeEntitySetId)
            val dstEntitySetIds = dstAssociationEntitySetIds.getValue(edgeEntitySetId)

            val edgeAssociationType = associationTypes.getValue(edgeEntitySetId)
            val srcEntityTypes = srcEntitySetIds.map { allSrcEntityTypes.getValue(it) }
            val dstEntityTypes = dstEntitySetIds.map { allDstEntityTypes.getValue(it) }

            // ensure, that src and dst entity types are part of src and dst entity types of AssociationType
            checkAllowedEntityTypesOfAssociation(
                    edgeAssociationType,
                    edgeEntitySetId,
                    srcEntityTypes,
                    srcEntitySetIds,
                    dstEntityTypes,
                    dstEntitySetIds
            )
        }
    }

    private fun checkAllowedEntityTypesOfAssociation(
            associationType: AssociationType,
            edgeEntitySetId: UUID,
            srcEntityTypes: Collection<UUID>,
            srcEntitySetIds: Set<UUID>,
            dstEntityTypes: Collection<UUID>,
            dstEntitySetIds: Set<UUID>
    ) {
        val isSrcNotAllowed = !associationType.src.containsAll(srcEntityTypes)
        val isDstNotAllowed = !associationType.dst.containsAll(dstEntityTypes)

        val isSrcDstNotAllowed = !(associationType.src + associationType.dst)
                .containsAll(srcEntityTypes + dstEntityTypes)

        if (associationType.isBidirectional) {
            if (isSrcDstNotAllowed) {
                throw IllegalArgumentException(
                        "One or more entity types of entity sets src=$srcEntitySetIds, dst=$dstEntitySetIds differs " +
                                "from allowed entity types src=${associationType.src}, dst=${associationType.dst} in " +
                                "bidirectional association type of entity set $edgeEntitySetId"
                )
            }
        } else {
            if (isSrcNotAllowed) {
                throw IllegalArgumentException(
                        "One or more entity types of src entity sets $srcEntitySetIds differs from allowed entity " +
                                "types (${associationType.src}) in association type of entity set $edgeEntitySetId"
                )
            }

            if (isDstNotAllowed) {
                throw IllegalArgumentException(
                        "One or more entity types of dst entity sets $dstEntitySetIds differs from allowed entity " +
                                "types (${associationType.dst}) in association type of entity set $edgeEntitySetId"
                )
            }
        }
    }
}