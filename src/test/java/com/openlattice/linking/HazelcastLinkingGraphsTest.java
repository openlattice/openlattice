

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

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.authorization.HzAuthzTest;
import com.openlattice.clustering.DistributedClusterer;
import com.openlattice.linking.HazelcastLinkingGraphs;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertex;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKeySet;
import com.openlattice.data.EntityKey;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.IMap;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.openlattice.postgres.mapstores.LinkingVerticesMapstore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HazelcastLinkingGraphsTest extends HzAuthzTest {
    protected static final int entityCount = 1000;
    protected static final HazelcastLinkingGraphs                              graphs;
    protected static final DistributedClusterer                                partitioner;
    protected static final LinkingVerticesMapstore                             lvm;
    protected static final IMap<LinkingVertexKey, WeightedLinkingVertexKeySet> edges;
    protected static final IMap<LinkingVertexKey, LinkingVertex>               linkingVertices;
    protected static final IMap<EntityKey, UUID>                               ids;
    @SuppressFBWarnings( value = "MS_MUTABLE_COLLECTION_PKGPROTECT", justification = "used is protected" )
    protected static final Set<UUID> used    = new HashSet<>( entityCount );
    private static final   Random    r       = new Random();
    private static final   Logger    logger  = LoggerFactory
            .getLogger( HazelcastLinkingGraphsTest.class );

    static {
        graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        edges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
        ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        linkingVertices = hazelcastInstance.getMap( HazelcastMap.LINKING_VERTICES.name() );
        partitioner = new DistributedClusterer( hazelcastInstance );
        lvm = new LinkingVerticesMapstore( hds );
    }

    @Test
    public void testClustering() {
        final UUID entitySetId = UUID.randomUUID();
        final UUID syncId = UUIDs.timeBased();

        for ( int i = 0; i < entityCount; i++ ) {
            UUID id = UUID.randomUUID();
            while ( used.contains( id ) ) {
                id = UUID.randomUUID();
            }
            ids.put( TestDataFactory.entityKey( entitySetId, syncId ), id );
        }

        ids.entrySet().stream().filter( entry -> entry.getKey().getEntitySetId().equals( entitySetId ) ).forEach( entry -> {
            linkingVertices.set( new LinkingVertexKey( entitySetId, entry.getValue() ),
                    new LinkingVertex( 0.0D, Sets.newHashSet( entry.getValue() ) ) );
        } );
        
        Set<LinkingVertexKey> vertices = linkingVertices.keySet().stream()
                .filter( key -> key.getGraphId().equals( entitySetId ) ).collect(
                        Collectors.toSet() );

        vertices.parallelStream()
                .flatMap( u -> vertices
                        .parallelStream()
                        .filter( v -> !u.equals( v ) && r.nextBoolean() && r.nextBoolean() && r.nextBoolean()
                                && r.nextBoolean() )
                        .map( v -> new LinkingEdge( u, v ) ) )
                .forEach( edge -> graphs.setEdgeWeight( edge, 2 * r.nextDouble() ) );

        List<WeightedLinkingVertexKey> sortedEdges = edges.entrySet()
                .stream()
                .filter( entry -> entry.getKey().getGraphId().equals( entitySetId ) )
                .flatMap( entry -> entry.getValue().stream() )
                .sorted()
                .collect( Collectors.toList() );

        partitioner.cluster( entitySetId, sortedEdges.get( 0 ).getWeight() );
        //                new WeightedLinkingEdge( sortedEdges.get( 1 ).getValue(), sortedEdges.get( 1 ).getAclKey() )

        StreamUtil.stream( lvm.loadAllKeys() ).map( graphs::getVertex )
                .sorted( Comparator.comparing( LinkingVertex::getDiameter ) )
                .peek( v -> Assert.assertTrue( v.getEntityKeys().size() > 0 ) )
                .forEach( v -> logger.info( "Cluster: {}", v ) );
    }

}
