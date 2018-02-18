/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.linking.aggregators;

import com.openlattice.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.openlattice.linking.*;
import com.dataloom.streams.StreamUtil;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.HazelcastLinkingGraphs;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.WeightedLinkingEdge;
import com.openlattice.linking.WeightedLinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKeySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MergingAggregator
        extends Aggregator<Entry<LinkingVertexKey, WeightedLinkingVertexKeySet>, Void>
        implements HazelcastInstanceAware {
    private static final long serialVersionUID = -6877009130127065543L;
    private static final Logger logger = LoggerFactory.getLogger( MergingAggregator.class );

    private final Map<UUID, Double>   srcNeighborWeights;
    private final Map<UUID, Double>   dstNeighborWeights;
    private final WeightedLinkingEdge lightest;

    private transient IMap<LinkingVertexKey, WeightedLinkingVertexKeySet> weightedEdges = null;
    private transient HazelcastLinkingGraphs                              graphs        = null;

    public MergingAggregator(
            WeightedLinkingEdge lightest,
            Map<UUID, Double> srcNeighborWeights,
            Map<UUID, Double> dstNeighborWeights ) {
        this.srcNeighborWeights = srcNeighborWeights;
        this.dstNeighborWeights = dstNeighborWeights;
        this.lightest = lightest;
    }

    public MergingAggregator( WeightedLinkingEdge lightest ) {
        this( lightest, new HashMap<>(), new HashMap<>() );
    }

    private boolean vertexKeyMatchesLightest( LinkingVertexKey vertexKey ) {
        return vertexKey.equals( lightest.getEdge().getSrc() ) || vertexKey.equals( lightest.getEdge().getDst() );
    }

    @Override
    public void accumulate( Entry<LinkingVertexKey, WeightedLinkingVertexKeySet> input ) {
        LinkingEdge lightestEdge = lightest.getEdge();
        boolean keyMatches = vertexKeyMatchesLightest( input.getKey() );

        for ( WeightedLinkingVertexKey k : input.getValue() ) {
            boolean valMatches = vertexKeyMatchesLightest( k.getVertexKey() );

            if ( keyMatches || valMatches ) {
                LinkingEdge edge = new LinkingEdge( input.getKey(), k.getVertexKey() );
                accumulateHelper( lightestEdge, edge, k.getWeight() );
                if ( !keyMatches ) {
                    weightedEdges.executeOnKey( input.getKey(),
                            new WeightedLinkingVertexKeyValueRemover( Arrays
                                    .asList( k ) ) );
                }
            }
        }
        if ( keyMatches ) {
            weightedEdges.delete( input.getKey() );
        }
    }

    void accumulateHelper( LinkingEdge lightestEdge, LinkingEdge edge, double weight ) {

        if ( !edge.equals( lightestEdge ) ) {
            UUID srcId = lightestEdge.getSrcId();
            UUID dstId = lightestEdge.getDstId();

            if ( srcId.equals( edge.getSrcId() ) ) {
                srcNeighborWeights.put( edge.getDstId(), weight );
            } else if ( srcId.equals( edge.getDstId() ) ) {
                srcNeighborWeights.put( edge.getSrcId(), weight );
            }

            if ( dstId.equals( edge.getSrcId() ) ) {
                dstNeighborWeights.put( edge.getDstId(), weight );
            } else if ( dstId.equals( edge.getDstId() ) ) {
                dstNeighborWeights.put( edge.getSrcId(), weight );
            }
        }
    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof MergingAggregator ) {
            MergingAggregator other = (MergingAggregator) aggregator;

            // TODO: At some point we might want to check and make sure there aren't duplicates.
            srcNeighborWeights.putAll( other.srcNeighborWeights );
            dstNeighborWeights.putAll( other.dstNeighborWeights );

        } else {
            logger.error( "Cannot combine incompatible aggregators." );
        }
    }

    @Override
    public Void aggregate() {
        final LinkingVertexKey vertexKey = graphs.merge( lightest );
        logger.info( "Merging: {}", lightest.getWeight() );
        weightedEdges.delete( lightest );
        if ( srcNeighborWeights.isEmpty() && dstNeighborWeights.isEmpty() ) {
            return null;
        }
        Stream<UUID> neighbors = Stream
                .concat( srcNeighborWeights.keySet().stream(), dstNeighborWeights.keySet().stream() );

        neighbors
                .map( neighbor -> agg( neighbor, vertexKey ) )
                .forEach( StreamUtil::getUninterruptibly );
        return null;
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        this.weightedEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
    }

    public Map<UUID, Double> getSrcNeighborWeights() {
        return srcNeighborWeights;
    }

    public Map<UUID, Double> getDstNeighborWeights() {
        return dstNeighborWeights;
    }

    public WeightedLinkingEdge getLightest() {
        return lightest;
    }

    private ListenableFuture agg( UUID neighbor, LinkingVertexKey vertexKey ) {
        final double lightestWeight = lightest.getWeight();
        final UUID graphId = lightest.getEdge().getGraphId();
        Double srcNeighborWeight = srcNeighborWeights.get( neighbor );
        Double dstNeighborWeight = dstNeighborWeights.get( neighbor );
        double minSrc;
        if ( srcNeighborWeight == null ) {
            minSrc = dstNeighborWeight.doubleValue() + lightestWeight;
        } else if ( dstNeighborWeight == null ) {
            minSrc = srcNeighborWeight.doubleValue();
        } else {
            minSrc = Math
                    .min( srcNeighborWeight.doubleValue(),
                            dstNeighborWeight.doubleValue() + lightestWeight );
        }

        double minDst;
        if ( srcNeighborWeight == null ) {
            minDst = dstNeighborWeight.doubleValue();
        } else if ( dstNeighborWeight == null ) {
            minDst = srcNeighborWeight.doubleValue() + lightestWeight;
        } else {
            minDst = Math
                    .min( srcNeighborWeight.doubleValue() + lightestWeight,
                            dstNeighborWeight.doubleValue() );
        }

        LinkingVertexKey neighborKey = new LinkingVertexKey( graphId, neighbor );

        double weight = Math.max( minSrc, minDst );

        return new ListenableHazelcastFuture( weightedEdges.submitToKey( vertexKey,
                new WeightedLinkingVertexKeyMerger( Arrays
                        .asList( new WeightedLinkingVertexKey( weight, neighborKey ) ) ) ) );
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof MergingAggregator ) ) {
            return false;
        }

        MergingAggregator that = (MergingAggregator) o;

        if ( !srcNeighborWeights.equals( that.srcNeighborWeights ) ) {
            return false;
        }
        if ( !dstNeighborWeights.equals( that.dstNeighborWeights ) ) {
            return false;
        }
        return lightest.equals( that.lightest );
    }

    @Override
    public int hashCode() {
        int result = srcNeighborWeights.hashCode();
        result = 31 * result + dstNeighborWeights.hashCode();
        result = 31 * result + lightest.hashCode();
        return result;
    }
}
