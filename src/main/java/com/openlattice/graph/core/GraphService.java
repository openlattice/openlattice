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

package com.openlattice.graph.core;

import com.google.common.collect.SetMultimap;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.graph.query.GraphQuery;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph.
 */
public interface GraphService {

    int createEdges( Set<EdgeKey> keys );

    int clearEdges( Set<EdgeKey> keys );

    int clearVerticesInEntitySet( UUID entitySetId );

    int clearVertices( UUID entitySetId, Set<UUID> vertices );

    int deleteEdges( Set<EdgeKey> keys );

    int deleteVerticesInEntitySet( UUID entitySetId );

    int deleteVertices( UUID entitySetId, Set<UUID> vertices );

    Edge getEdge( EdgeKey key );

    Map<EdgeKey, Edge> getEdgesAsMap( Set<EdgeKey> keys );

    Stream<Edge> getEdges( Set<EdgeKey> keys );

    Stream<Edge> getEdgesAndNeighborsForVertex( UUID entitySetId, UUID vertexId );

    Stream<Edge> getEdgesAndNeighborsForVertices( UUID entitySetId, Set<UUID> vertexIds );

    Stream<EntityDataKey> topEntities(
            int limit,
            UUID entitySetId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters );

    /**
     * @param srcFilters Association type ids to neighbor type ids
     * @param dstFilters Association type ids to neighbor type ids
     */
    IncrementableWeightId[] computeGraphAggregation(
            int limit,
            UUID entitySetId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters );

    List<NeighborSets> getNeighborEntitySets( UUID entitySetId );
}