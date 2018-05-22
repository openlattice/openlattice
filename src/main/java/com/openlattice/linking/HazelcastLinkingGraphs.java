

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

package com.openlattice.linking;

import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.openlattice.data.EntityKey;
import com.openlattice.data.hazelcast.EntitySets;
import com.openlattice.datastore.util.Util;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.HazelcastUtils;
import com.openlattice.linking.aggregators.WeightedLinkingVertexKeyMerger;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

/**
 * Implements a multiple simple graphs over by imposing a canonical ordering on vertex order for linkingEdges.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HazelcastLinkingGraphs {
    private static final UUID DEFAULT_ID = new UUID( 0, 0 );
    private final IMap<LinkingVertexKey, LinkingVertex>               linkingVertices;
    private final IMap<LinkingVertexKey, WeightedLinkingVertexKeySet> weightedEdges;
    private final IMap<EntityKey, UUID>                               ids;

    public HazelcastLinkingGraphs( HazelcastInstance hazelcastInstance ) {
        this.linkingVertices = hazelcastInstance.getMap( HazelcastMap.LINKING_VERTICES.name() );
        this.weightedEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
    }

    public ListenableFuture setEdgeWeightAsync( LinkingEdge edge, double weight ) {
        return new ListenableHazelcastFuture( weightedEdges.submitToKey( edge.getSrc(),
                new WeightedLinkingVertexKeyMerger(
                        Arrays
                                .asList( new WeightedLinkingVertexKey( weight, edge.getDst() ) ) ) ) );
    }

    public void setEdgeWeight( LinkingEdge edge, double weight ) {
        weightedEdges.executeOnKey( edge.getSrc(),
                new WeightedLinkingVertexKeyMerger(
                        Arrays
                                .asList( new WeightedLinkingVertexKey( weight, edge.getDst() ) ) ) );
    }

    public UUID getGraphIdFromEntitySetId( UUID linkedEntitySetId ) {
        return linkedEntitySetId;
    }

    public void initializeLinking( UUID graphId, Iterable<UUID> entitySetIds ) {
        ids.aggregate( new Initializer( graphId ), EntitySets.filterByEntitySetIds( entitySetIds ) );
    }

    public LinkingVertexKey merge( WeightedLinkingEdge weightedEdge ) {
        LinkingEdge edge = weightedEdge.getEdge();
        LinkingVertex u = Util.getSafely( linkingVertices, edge.getSrc() );
        LinkingVertex v = Util.getSafely( linkingVertices, edge.getDst() );
        Set<UUID> entityKeys = Sets
                .newHashSetWithExpectedSize( u.getEntityKeys().size() + v.getEntityKeys().size() );
        entityKeys.addAll( u.getEntityKeys() );
        entityKeys.addAll( v.getEntityKeys() );
        /*
         * As long as min edge is chosen for merging it is appropriate to use the edge weight as new diameter.
         */

        deleteVertex( edge.getSrc() );
        deleteVertex( edge.getDst() );

        return HazelcastUtils.insertIntoUnusedKey( linkingVertices,
                new LinkingVertex( weightedEdge.getWeight(), entityKeys ),
                () -> new LinkingVertexKey( edge.getGraphId(), UUID.randomUUID() ) );
    }

    public LinkingVertex getVertex( LinkingVertexKey vertexKey ) {
        return linkingVertices.get( vertexKey );
    }

    public void deleteVertex( LinkingVertexKey key ) {
        Util.deleteSafely( linkingVertices, key );
    }

    public boolean verticesExists( LinkingEdge edge ) {
        return linkingVertices.containsKey( edge.getSrc() ) && linkingVertices.containsKey( edge.getDst() );
    }

    public static class Initializer extends Aggregator<Entry<EntityKey, UUID>, Void> implements HazelcastInstanceAware {
        private static final long serialVersionUID           = 690840541802755664L;
        private static final int  MAX_FAILED_CONSEC_ATTEMPTS = 5;

        public            UUID                     graphId;
        private transient HazelcastBlockingService blockingService;
        private transient ICountDownLatch          countDownLatch;

        public Initializer( UUID graphId ) {
            this.graphId = graphId;
        }

        public Initializer( UUID graphId, HazelcastBlockingService blockingService ) {
            this.graphId = graphId;
            this.blockingService = blockingService;
        }

        @Override public void accumulate( Entry<EntityKey, UUID> input ) {
            blockingService.setLinkingVertex( graphId, input.getValue() );
        }

        @Override public void combine( Aggregator aggregator ) {
        }

        @Override public Void aggregate() {
            int numConsecFailures = 0;
            long count = countDownLatch.getCount();
            while ( count > 0 && numConsecFailures < MAX_FAILED_CONSEC_ATTEMPTS ) {
                try {
                    Thread.sleep( 5000 );
                    long newCount = countDownLatch.getCount();
                    if ( newCount == count ) {
                        System.err.println( "Nothing is happening." );
                        numConsecFailures++;
                    } else
                        numConsecFailures = 0;
                    count = newCount;
                } catch ( InterruptedException e ) {
                    System.err.println( "Error occurred while waiting for linking vertices to initialize." );
                }
            }
            return null;
        }

        @Override
        public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
            this.countDownLatch = hazelcastInstance.getCountDownLatch( graphId.toString() );
        }

        public UUID getGraphId() {
            return graphId;
        }

    }

}