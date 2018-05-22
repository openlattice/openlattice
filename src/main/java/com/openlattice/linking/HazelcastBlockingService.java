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

import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.openlattice.blocking.GraphEntityPair;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.predicates.LinkingPredicates;
import com.openlattice.matching.FeatureExtractionAggregator;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.scheduling.annotation.Async;

import javax.inject.Inject;
import java.util.Map;
import java.util.UUID;

public class HazelcastBlockingService {

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    private static final int     blockSize = 50;
    private static final boolean explain   = false;

    private IMap<GraphEntityPair, LinkingEntity>  linkingEntities;
    private IMap<LinkingVertexKey, LinkingVertex> linkingVertices;
    private HazelcastInstance                     hazelcastInstance;

    public HazelcastBlockingService( HazelcastInstance hazelcastInstance ) {
        this.linkingEntities = hazelcastInstance.getMap( HazelcastMap.LINKING_ENTITIES.name() );
        this.linkingVertices = hazelcastInstance.getMap( HazelcastMap.LINKING_VERTICES.name() );
        this.hazelcastInstance = hazelcastInstance;
    }

    @Async
    public void setLinkingVertex( UUID graphId, UUID vertexId ) {
        linkingVertices.set( new LinkingVertexKey( graphId, vertexId ),
                new LinkingVertex( 0.0D, Sets.newHashSet( vertexId ) ) );
        hazelcastInstance.getCountDownLatch( graphId.toString() ).countDown();
    }

    @Async
    public void blockAndMatch(
            GraphEntityPair graphEntityPair,
            LinkingEntity linkingEntity,
            Iterable<UUID> entitySetIds,
            Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn ) {
        UUID[] entityKeyIds = elasticsearchApi
                .executeEntitySetDataSearchAcrossIndices( entitySetIds,
                        linkingEntity.getEntity(),
                        blockSize,
                        explain ).toArray( new UUID[] {} );
        linkingEntities.aggregate( new FeatureExtractionAggregator( graphEntityPair,
                        linkingEntity,
                        propertyTypeIdIndexedByFqn ),
                LinkingPredicates.entitiesFromEntityKeyIdsAndGraphId( entityKeyIds, graphEntityPair.getGraphId() ) );
        hazelcastInstance.getCountDownLatch( graphEntityPair.getGraphId().toString() ).countDown();
    }
}
