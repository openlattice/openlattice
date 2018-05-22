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

package com.openlattice.matching;

import com.google.common.base.Stopwatch;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.openlattice.blocking.BlockingAggregator;
import com.openlattice.blocking.GraphEntityPair;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.blocking.LoadingAggregator;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.hazelcast.EntitySets;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.HazelcastLinkingGraphs;
import com.openlattice.linking.predicates.LinkingPredicates;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.scheduling.annotation.Async;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DistributedMatcher {
    private EdmManager dms;

    private       SetMultimap<UUID, UUID>              linkIndexedByEntitySets;
    private       Iterable<UUID>                       linkingEntitySetIds;
    private       Map<FullQualifiedName, UUID>         propertyTypeIdIndexedByFqn;
    private       IMap<EntityDataKey, EntityDataValue> entities;
    private final IMap<GraphEntityPair, LinkingEntity> linkingEntities;
    private final HazelcastInstance                    hazelcast;
    private final HazelcastLinkingGraphs               graphs;

    public DistributedMatcher(
            HazelcastInstance hazelcast,
            EdmManager dms ) {
        this.linkingEntities = hazelcast.getMap( HazelcastMap.LINKING_ENTITIES.name() );
        this.entities = hazelcast.getMap( HazelcastMap.ENTITY_DATA.name() );
        this.dms = dms;
        this.graphs = new HazelcastLinkingGraphs( hazelcast );
        this.hazelcast = hazelcast;
    }

    public double match( UUID graphId ) {
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = linkIndexedByEntitySets.asMap().entrySet().stream()
                .collect( Collectors.toMap( entry -> entry.getKey(),
                        entry -> entry.getValue().stream()
                                .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) ) ) );

        Stopwatch s = Stopwatch.createStarted();

        graphs.initializeLinking( graphId, linkingEntitySetIds );

        int numEntities = entities.aggregate( new LoadingAggregator( graphId, authorizedPropertyTypes ),
                EntitySets.filterByEntitySetIds( linkingEntitySetIds ) );
        System.out.println( "t1: " + String.valueOf( s.elapsed( TimeUnit.MILLISECONDS ) ) );
        s.reset();
        s.start();

        ICountDownLatch latch = hazelcast.getCountDownLatch( graphId.toString() );
        latch.trySetCount( numEntities );

        linkingEntities
                .aggregate( new BlockingAggregator( graphId, linkingEntitySetIds, propertyTypeIdIndexedByFqn ),
                        LinkingPredicates.graphId( graphId ) );
        System.out.println( "t2: " + String.valueOf( s.elapsed( TimeUnit.MILLISECONDS ) ) );
        cleanLinkingEntitiesMap( graphId );
        latch.destroy();
        return 0.5;
    }

    @Async
    private void cleanLinkingEntitiesMap( UUID graphId ) {
        linkingEntities.removeAll( LinkingPredicates.graphId( graphId ) );
    }

    public void setLinking(
            Iterable<UUID> linkingEntitySetIds,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
        this.linkingEntitySetIds = linkingEntitySetIds;
        this.linkIndexedByEntitySets = linkIndexedByEntitySets;
        this.propertyTypeIdIndexedByFqn = getPropertyTypeIdIndexedByFqn( linkIndexedByPropertyTypes.keySet() );

    }

    private Map<FullQualifiedName, UUID> getPropertyTypeIdIndexedByFqn( Set<UUID> propertyTypeIds ) {
        return propertyTypeIds.stream()
                .collect( Collectors.toMap( id -> dms.getPropertyType( id ).getType(), id -> id ) );
    }
}
