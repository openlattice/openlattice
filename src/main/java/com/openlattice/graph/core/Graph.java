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

import com.codahale.metrics.annotation.Timed;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.graph.aggregators.GraphCount;
import com.openlattice.graph.aggregators.NeighborEntitySetAggregator;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public class Graph implements GraphApi {

    private final ListeningExecutorService executor;
    private final IMap<EdgeKey, Edge>      edges;

    // vertex id -> dst type id -> edge type id -> dst entity key id
    // private final IMap<UUID, Neighborhood> backedges;

    // private final IMap<UUID, Neighborhood> edges;
    // // vertex id -> dst type id -> edge type id -> dst entity key id
    // private final IMap<UUID, Neighborhood> backedges;

    public Graph( ListeningExecutorService executor, HazelcastInstance hazelcastInstance ) {
        this.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );
        this.executor = executor;
    }

    @Override
    public void addEdge(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {
        StreamUtil.getUninterruptibly( addEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                dstVertexId,
                dstVertexEntityTypeId,
                dstVertexEntitySetId,
                edgeEntityId,
                edgeEntityTypeId,
                edgeEntitySetId ) );
    }

    @Override
    public ListenableFuture<Void> addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID srcVertexEntitySetId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID dstVertexEntitySetId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId,
            UUID edgeEntitySetId ) {

        EdgeKey key = new EdgeKey( srcVertexId, dstVertexEntityTypeId, edgeEntityTypeId, dstVertexId, edgeEntityId );
        Edge edge = new Edge(
                key,
                srcVertexEntityTypeId,
                srcVertexEntitySetId,
                dstVertexEntitySetId,
                edgeEntitySetId );

        return new ListenableHazelcastFuture<>( edges.setAsync( key, edge ) );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        StreamUtil.getUninterruptibly( deleteVertexAsync( vertexId ) );
    }

    @Override
    public ListenableFuture deleteVertexAsync( UUID vertex ) {
        return executor.submit( () -> edges.removeAll( neighborhood( vertex ) ) );
    }

    @Override
    public Edge getEdge( EdgeKey key ) {
        return edges.get( key );
    }

    @Override
    public Void submitAggregator( Aggregator<Entry<EdgeKey, Edge>, Void> agg, Predicate p ) {
        return edges.aggregate( agg, p );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        StreamUtil.getUninterruptibly( deleteEdgeAsync( key ) );
    }

    @Override
    public ListenableFuture deleteEdgeAsync( EdgeKey edgeKey ) {
        return executor.submit( () -> edges.delete( edgeKey ) );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        edges.removeAll( Predicates.equal( "srcEntityKeyId", srcId ) );
    }

    @Override
    @Timed
    public Stream<Edge> getEdgesAndNeighborsForVertex( UUID vertexId ) {
        return edges.values( neighborhood( vertexId ) ).stream();
    }

    @Override
    @Timed
    public Stream<Edge> getEdgesAndNeighborsForVertices( Set<UUID> vertexIds ) {
        return edges.values( Predicates.or( Predicates.in( "srcEntityKeyId", vertexIds.toArray( new UUID[] {} ) ),
                Predicates.in( "dstEntityKeyId", vertexIds.toArray( new UUID[] {} ) ) ) ).stream();
    }

    @Override
    @Timed
    public IncrementableWeightId[] computeGraphAggregation(
            int limit,
            UUID entitySetId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters ) {
        Predicate p = edgesMatching( entitySetId, srcFilters, dstFilters );
        return this.edges.aggregate( new GraphCount( limit, entitySetId ), p );
    }

    @Override
    @Timed
    public NeighborTripletSet getNeighborEntitySets( UUID entitySetId ) {
        Predicate p = Predicates.or(
                Predicates.and( Predicates.equal( "srcSetId", entitySetId ) ),
                Predicates.and( Predicates.equal( "dstSetId", entitySetId ) )
        );
        return edges.aggregate( new NeighborEntitySetAggregator(), p );
    }

    static Predicate neighborhood( UUID entityKeyId ) {
        return Predicates.or(
                Predicates.equal( "dstEntityKeyId", entityKeyId ),
                Predicates.equal( "srcEntityKeyId", entityKeyId ) );
    }

    public static Predicate edgesMatching(
            UUID entitySetId,
            SetMultimap<UUID, UUID> srcFilters,
            SetMultimap<UUID, UUID> dstFilters ) {
        /*
         * No need to execute on back edge map we are looking for items in specified entity set that have incoming edges
         * of a given type from a given destination type. That means srcType = We are looking for anything of that type
         * id to the src entity set -> dst where
         */
        return Predicates.or(
                Stream.concat( dstFilters.entries().stream()
                                .map( dstFilter -> Predicates.and(
                                        Predicates.equal( "dstSetId", entitySetId ),
                                        Predicates.equal( "edgeTypeId", dstFilter.getKey() ),
                                        Predicates.equal( "srcTypeId", dstFilter.getValue() ) ) ),
                        srcFilters.entries().stream()
                                .map( srcFilter -> Predicates.and(
                                        Predicates.equal( "srcSetId", entitySetId ),
                                        Predicates.equal( "edgeTypeId", srcFilter.getKey() ),
                                        Predicates.equal( "dstTypeId", srcFilter.getValue() ) ) ) )
                        .toArray( Predicate[]::new ) );

    }

}
