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
import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking;
import com.openlattice.data.DataEdgeKey;
import com.openlattice.data.WriteEvent;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.edge.Edge;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.search.requests.EntityNeighborsFilter;

import java.util.*;
import java.util.stream.Stream;

/**
 * Graph Object supporting CRUD operations of vertices and edges to the graph.
 */
public interface GraphService {

    WriteEvent createEdges( Set<DataEdgeKey> keys );

    int clearEdges( Set<DataEdgeKey> keys );

    int clearVerticesInEntitySet( UUID entitySetId );

    int clearVertices( UUID entitySetId, Set<UUID> vertices );

    WriteEvent deleteEdges( Set<DataEdgeKey> keys );

    int deleteVerticesInEntitySet( UUID entitySetId );

    int deleteVertices( UUID entitySetId, Set<UUID> vertices );

    Edge getEdge( DataEdgeKey key );

    Map<DataEdgeKey, Edge> getEdgesAsMap( Set<DataEdgeKey> keys );

    Stream<Edge> getEdges( Set<DataEdgeKey> keys );

    PostgresIterable<DataEdgeKey> getEdgeKeysOfEntitySet( UUID entitySetId );

    PostgresIterable<DataEdgeKey> getEdgeKeysContainingEntities( UUID entitySetId, Set<UUID> entityKeyIds );

    Iterable<DataEdgeKey> getEdgeKeysContainingEntity( UUID entitySetId, UUID entityKeyId );

    Stream<Edge> getEdgesAndNeighborsForVertex( UUID entitySetId, UUID vertexId );

    Stream<Edge> getEdgesAndNeighborsForVertices( UUID entitySetId, EntityNeighborsFilter filter );

    Stream<Edge> getEdgesAndNeighborsForVerticesBulk(Set<UUID> entitySetIds, EntityNeighborsFilter filter);

    Stream<IncrementableWeightId> topEntitiesOld(
            int limit,
            UUID entitySetId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters );

    PostgresIterable<Map<String,Object>> computeTopEntities(
            int limit,
            Set<UUID> entitySetIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes,
            List<AuthorizedFilteredNeighborsRanking> details,
            boolean linked,
            Optional<UUID> linkingEntitySetId);

    /**
     * @param srcFilters Association type ids to neighbor entity set ids
     * @param dstFilters Association type ids to neighbor entity set ids
     */
    IncrementableWeightId[] computeGraphAggregation(
            int limit,
            UUID entitySetId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters );

    List<NeighborSets> getNeighborEntitySets( Set<UUID> entitySetIds );

    PostgresIterable<DataEdgeKey> getEntitiesForDestination(
            List<UUID> srcEntitySetIds,
            List<UUID> edgeEntitySetIds,
            Set<UUID> dstEntityKeyIds);
}