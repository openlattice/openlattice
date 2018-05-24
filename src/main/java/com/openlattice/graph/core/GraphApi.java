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
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.query.Predicate;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph.
 *
 */
public interface GraphApi {

    //    /*
    //     * CRUD operations of vertices
    //     */
    //    void createVertex( UUID vertexId );
    //
    //    ResultSetFuture createVertexAsync( UUID vertexId );
    //
    void deleteVertex( UUID vertexId );

    ListenableFuture deleteVertexAsync( UUID vertexId );

    /*
     * CRUD operations of edges
     */
    void addEdge(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeId,
            UUID edgeTypeId,
            UUID edgeEntitySetId );

    ListenableFuture addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId );

    /**
     * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
     * destination vertexId, and the edge syncId.
     */
    Edge getEdge( EdgeKey key );

    Void submitAggregator( Aggregator<Entry<EdgeKey, Edge>, Void> agg, Predicate p );

    void deleteEdge( EdgeKey edgeKey );

    ListenableFuture deleteEdgeAsync( EdgeKey edgeKey );

    void deleteEdges( UUID srcId );

    Stream<Edge> getEdgesAndNeighborsForVertex( UUID vertexId );

    Stream<Edge> getEdgesAndNeighborsForVertices( Set<UUID> vertexIds );

    IncrementableWeightId[] computeGraphAggregation(
            int limit,
            UUID entitySetId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters );

    NeighborTripletSet getNeighborEntitySets( UUID entitySetId );

}