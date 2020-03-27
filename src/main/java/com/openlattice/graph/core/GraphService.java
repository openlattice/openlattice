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

import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking;
import com.openlattice.data.DataEdgeKey;
import com.openlattice.data.WriteEvent;
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

    int clearEdges( Iterable<DataEdgeKey> keys );

    WriteEvent deleteEdges( Iterable<DataEdgeKey> keys );

    /**
     * Returns all {@link DataEdgeKey}s where either src, dst and/or edge entity set id(s) equal the requested
     * entitySetId.
     * If includeClearedEdges is set to true, it will also return cleared (version < 0) entities.
     */
    PostgresIterable<DataEdgeKey> getEdgeKeysOfEntitySet( UUID entitySetId, boolean includeClearedEdges );

    /**
     * Returns all {@link DataEdgeKey}s that include requested entityKeyIds either as src, dst and/or edge.
     * If includeClearedEdges is set to true, it will also return cleared (version < 0) entities.
     */
    PostgresIterable<DataEdgeKey> getEdgeKeysContainingEntities(
            UUID entitySetId,
            Set<UUID> entityKeyIds,
            boolean includeClearedEdges );

    Stream<Edge> getEdgesAndNeighborsForVertex( UUID entitySetId, UUID vertexId );

    Stream<Edge> getEdgesAndNeighborsForVertices( UUID entitySetId, EntityNeighborsFilter filter );

    Stream<Edge> getEdgesAndNeighborsForVerticesBulk( Set<UUID> entitySetIds, EntityNeighborsFilter filter );

    Set<UUID> getEdgeEntitySetsConnectedToEntities( UUID entitySetId, Set<UUID> entityKeyIds );

    Set<UUID> getEdgeEntitySetsConnectedToEntitySet( UUID entitySetId );

    PostgresIterable<Map<String, Object>> computeTopEntities(
            int limit,
            Set<UUID> entitySetIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes,
            List<AuthorizedFilteredNeighborsRanking> details,
            boolean linked,
            Optional<UUID> linkingEntitySetId );

    List<NeighborSets> getNeighborEntitySets( Set<UUID> entitySetIds );
}