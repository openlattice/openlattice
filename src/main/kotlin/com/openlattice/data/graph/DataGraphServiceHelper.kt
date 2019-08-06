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
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.AssociationType
import java.util.UUID

class DataGraphServiceHelper(private val edmManager: EdmManager) {

    fun checkEdgeEntityTypes(edges: Set<DataEdgeKey>) {
        //Create graph structure and check entity types
        val srcAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-src
        val dstAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-dst

        edges.stream().forEach { association ->
            val srcEsId = association.src.entitySetId
            val dstEsId = association.dst.entitySetId
            val edgeEsId = association.edge.entitySetId

            if (srcAssociationEntitySetIds.putIfAbsent(edgeEsId, mutableSetOf(srcEsId)) != null) {
                srcAssociationEntitySetIds.getValue(edgeEsId).add(srcEsId)
            }

            if (dstAssociationEntitySetIds.putIfAbsent(edgeEsId, mutableSetOf(dstEsId)) != null) {
                dstAssociationEntitySetIds.getValue(edgeEsId).add(dstEsId)
            }
        }

        checkAssociationEntityTypes(srcAssociationEntitySetIds, dstAssociationEntitySetIds)
    }

    fun checkAssociationEntityTypes(associations: Set<Association>) {
        //Create graph structure and check entity types
        val srcAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-src
        val dstAssociationEntitySetIds = mutableMapOf<UUID, MutableSet<UUID>>() // edge-dst

        associations.stream().forEach { association ->
            val srcEsId = association.src.entitySetId
            val dstEsId = association.dst.entitySetId
            val edgeEsId = association.key.entitySetId

            if (srcAssociationEntitySetIds.putIfAbsent(edgeEsId, mutableSetOf(srcEsId)) != null) {
                srcAssociationEntitySetIds.getValue(edgeEsId).add(srcEsId)
            }

            if (dstAssociationEntitySetIds.putIfAbsent(edgeEsId, mutableSetOf(dstEsId)) != null) {
                dstAssociationEntitySetIds.getValue(edgeEsId).add(dstEsId)
            }

        }

        checkAssociationEntityTypes(srcAssociationEntitySetIds, dstAssociationEntitySetIds)
    }


    /**
     * Checks entity types of associations against the allowed src and dst entity types in association type.
     * @param srcAssociationEntitySetIds The entity set ids of the src entities in associations mapped by their association entity set id.
     * @param dstAssociationEntitySetIds The entity set ids of the dst entities in associations mapped by their association entity set id.
     */
    private fun checkAssociationEntityTypes(
            srcAssociationEntitySetIds: Map<UUID, Set<UUID>>,
            dstAssociationEntitySetIds: Map<UUID, Set<UUID>>
    ) {
        val associationTypes = edmManager.getAssociationTypeDetailsByEntitySetIds(srcAssociationEntitySetIds.keys)
        associationTypes.keys.forEach { edgeEntitySetId ->
            val srcEntitySetIds = srcAssociationEntitySetIds.getValue(edgeEntitySetId)
            val dstEntitySetIds = dstAssociationEntitySetIds.getValue(edgeEntitySetId)

            val edgeAssociationType = associationTypes.getValue(edgeEntitySetId)
            val srcEntityTypes = edmManager.getEntityTypeIdsByEntitySetIds(srcEntitySetIds).values
            val dstEntityTypes = edmManager.getEntityTypeIdsByEntitySetIds(dstEntitySetIds).values

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

    fun checkAssociationEntityTypes(associations: ListMultimap<UUID, DataEdge>) {
        associations.asMap().forEach {
            // check entity types of associations before creation
            checkAssociationEntityTypes(it.key, it.value)
        }
    }

    // TODO improve perf
    /**
     * Checks whether the entity type of the src and dst entity sets in each association are part of allowed src and dst
     * entity types of the association entity type.
     */
    private fun checkAssociationEntityTypes(associationEntitySetId: UUID, associations: Collection<DataEdge>) {
        val associationType = edmManager.getAssociationTypeByEntitySetId(associationEntitySetId)
        if (associationType.associationEntityType.id == edmManager.auditRecordEntitySetsManager.auditingTypes.auditingEdgeEntityTypeId) {
            return
        }

        associations.forEach {
            // ensure, that DataEdge src and dst entity types are part of src and dst entity types of AssociationType
            val srcEntityType = edmManager.getEntityTypeByEntitySetId(it.src.entitySetId)
            val dstEntityType = edmManager.getEntityTypeByEntitySetId(it.dst.entitySetId)

            checkAllowedEntityTypesOfAssociation(
                    associationType,
                    associationEntitySetId,
                    setOf(srcEntityType.id),
                    setOf(it.src.entitySetId),
                    setOf(dstEntityType.id),
                    setOf(it.dst.entitySetId))
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

        val isSrcDstNotAllowed = !(associationType.src + associationType.dst).containsAll(srcEntityTypes + dstEntityTypes)

        if (associationType.isBidirectional) {
            if (isSrcDstNotAllowed) {
                throw IllegalArgumentException(
                        "One or more entity types of entity sets src=$srcEntitySetIds, dst=$dstEntitySetIds differs " +
                                "from allowed entity types (${associationType.src + associationType.dst}) in " +
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