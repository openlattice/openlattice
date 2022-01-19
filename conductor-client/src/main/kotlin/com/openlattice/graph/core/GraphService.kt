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
 */
package com.openlattice.graph.core

import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking
import com.openlattice.analysis.requests.AggregationResult
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.WriteEvent
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.PagedNeighborRequest
import com.openlattice.graph.edge.Edge
import com.geekbeast.postgres.streams.BasePostgresIterable
import java.util.*
import java.util.stream.Stream

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph.
 */
interface GraphService {
    fun createEdges(keys: Set<DataEdgeKey>): WriteEvent

    fun clearEdges(keys: Iterable<DataEdgeKey>): Int

    fun deleteEdges(keys: Iterable<DataEdgeKey>, deleteType: DeleteType  = DeleteType.Soft ): WriteEvent

    /**
     * Returns all [DataEdgeKey]s that include requested entityKeyIds either as src, dst and/or edge.
     * If includeClearedEdges is set to true, it will also return cleared (version < 0) entities.
     */
    fun getEdgeKeysContainingEntities(
        entitySetId: UUID,
        entityKeyIds: Set<UUID>,
        includeClearedEdges: Boolean
    ): BasePostgresIterable<DataEdgeKey>

    fun getEdgesAndNeighborsForVertices(
        entitySetIds: Set<UUID>,
        pagedNeighborRequest: PagedNeighborRequest
    ): Stream<Edge>

    fun getEdgeEntitySetsConnectedToEntities(
        entitySetId: UUID,
        entityKeyIds: Set<UUID>
    ): Set<UUID>

    // returns a set of edge entity sets where either src or dst is in any of the specified entity set ids
    fun getNeighborEdgeEntitySets(
            entitySetIds: Set<UUID>,
            entityKeyIds: Set<UUID>? = null
    ): Set<UUID>

    fun getNeighborEntitySets(entitySetIds: Set<UUID>): List<NeighborSets>

    fun checkForUnauthorizedEdges(
        entitySetId: UUID,
        authorizedEdgeEntitySets: Set<UUID>,
        entityKeyIds: Set<UUID>?
    ): Boolean
}
