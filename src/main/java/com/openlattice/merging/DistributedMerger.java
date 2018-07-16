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

package com.openlattice.merging;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.openlattice.data.DatasourceManager;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.HazelcastListingService;
import com.openlattice.linking.LinkingVertex;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.aggregators.CountVerticesAggregator;
import com.openlattice.linking.aggregators.MergeVertexAggregator;
import com.openlattice.linking.predicates.LinkingPredicates;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedMerger {
    private static final Logger logger = LoggerFactory.getLogger( DistributedMerger.class );
    private final IMap<LinkingVertexKey, LinkingVertex> linkingVertices;
    private final IMap<EdgeKey, Edge>                   edges;
    private final HazelcastListingService               listingService;
    private final EdmManager                            dms;
    private final DatasourceManager                     datasourceManager;
    private final HazelcastInstance                     hazelcast;

    public DistributedMerger(
            HazelcastInstance hazelcast,
            HazelcastListingService listingService,
            EdmManager dms,
            DatasourceManager datasourceManager ) {
        this.linkingVertices = hazelcast.getMap( HazelcastMap.LINKING_VERTICES.name() );
        this.edges = hazelcast.getMap( HazelcastMap.EDGES.name() );
        this.listingService = listingService;
        this.dms = dms;
        this.datasourceManager = datasourceManager;
        this.hazelcast = hazelcast;
    }

    public void merge(
            UUID graphId,
            Set<UUID> ownablePropertyTypes,
            Set<UUID> propertyTypesToPopulate ) {
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets = new HashMap<>();

        // compute authorized property types for each of the linking entity sets, as well as the linked entity set
        // itself
        Set<UUID> linkingSets = listingService.getLinkedEntitySets( graphId );
        Iterable<UUID> involvedEntitySets = Iterables.concat( linkingSets, ImmutableSet.of( graphId ) );
        for ( UUID esId : involvedEntitySets ) {
            Set<UUID> propertiesOfEntitySet = dms.getEntityTypeByEntitySetId( esId ).getProperties();
            Set<UUID> authorizedProperties = Sets.intersection( ownablePropertyTypes, propertiesOfEntitySet );

            Map<UUID, PropertyType> authorizedPropertyTypes = authorizedProperties.stream()
                    .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );

            authorizedPropertyTypesForEntitySets.put( esId, authorizedPropertyTypes );
        }

        UUID syncId = datasourceManager.getCurrentSyncId( graphId );
        mergeVertices( graphId, syncId, authorizedPropertyTypesForEntitySets, propertyTypesToPopulate );
        mergeEdges( graphId, linkingSets, syncId );

    }

    private void mergeVertices(
            UUID graphId,
            UUID syncId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets,
            Set<UUID> propertyTypesToPopulate ) {
        logger.debug( "Linking: Merging vertices..." );

        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet = Maps.transformValues(
                authorizedPropertyTypesForEntitySets.get( graphId ), pt -> pt.getDatatype() );
        // EntityType.getAclKey returns an unmodifiable view of the underlying linked hash set, so the order is still
        // preserved, although

        Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet = Maps.newHashMap();
        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();

        authorizedPropertyTypesForEntitySets.entrySet().forEach( entry -> {
            propertyTypeIdsByEntitySet.put( entry.getKey(), entry.getValue().keySet() );
            propertyTypesById.putAll( entry.getValue() );
        } );

        int numVertices = linkingVertices
                .aggregate( new CountVerticesAggregator(), LinkingPredicates.graphId( graphId ) );
        ICountDownLatch latch = hazelcast.getCountDownLatch( graphId.toString() );
        latch.trySetCount( numVertices );

        linkingVertices.aggregate( new MergeVertexAggregator( graphId,
                syncId,
                propertyTypeIdsByEntitySet,
                propertyTypesById,
                propertyTypesToPopulate,
                authorizedPropertiesWithDataTypeForLinkedEntitySet ), LinkingPredicates.graphId( graphId ) );
    }

    private void mergeEdges( UUID linkedEntitySetId, Set<UUID> linkingSets, UUID syncId ) {
        logger.debug( "Linking: Merging edges..." );
        logger.debug( "Linking Sets: {}", linkingSets );
        UUID[] ids = linkingSets.toArray( new UUID[ 0 ] );

//        Aggregator<Map.Entry<EdgeKey, Edge>, Void> agg = new MergeEdgeAggregator( linkedEntitySetId, syncId );
//        edges.aggregate( agg, Predicates.or( Predicates.in( PostgresEdgeMapstore.SRC_SET_ID, ids ),
//                Predicates.in( PostgresEdgeMapstore.DST_SET_ID, ids ),
//                Predicates.in( PostgresEdgeMapstore.EDGE_SET_ID, ids ) ) );
    }

}
