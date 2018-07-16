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
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.ForbiddenException;
import com.openlattice.blocking.GraphEntityPair;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.EntityKey;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.util.Util;
import com.openlattice.graph.Graph;
import com.openlattice.graph.edge.Edge;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.processors.EntityDataUpserter;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.zaxxer.hikari.HikariDataSource;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

public class HazelcastMergingService {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastMergingService.class );
    private final EntityKeyIdService                   ekIds;
    @Inject
    private       ConductorElasticsearchApi            elasticsearchApi;
    private       IMap<GraphEntityPair, LinkingEntity> linkingEntities;
    private       IMap<EntityDataKey, EntityDataValue> entities;
    private       IMap<LinkingVertexKey, UUID>         newIds;
    private       IMap<EntityKey, UUID>                ids;
    private       Graph                                graph;
    private       HazelcastInstance                    hazelcastInstance;
    private       ObjectMapper                         mapper;

    public HazelcastMergingService( HazelcastInstance hazelcastInstance, HikariDataSource hds, EdmManager edm ) {
        this.entities = hazelcastInstance.getMap( HazelcastMap.DATA.name() );
        this.newIds = hazelcastInstance.getMap( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name() );
        this.mapper = ObjectMappers.getJsonMapper();

        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        this.ekIds = new PostgresEntityKeyIdService( hazelcastInstance,
                hds,
                new HazelcastIdGenerationService( hazelcastInstance ) );
        this.graph = new Graph( hds, edm );
        this.hazelcastInstance = hazelcastInstance;

    }

    private SetMultimap<UUID, Object> computeMergedEntity(
            Set<UUID> entityKeyIds,
            Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet,
            Set<UUID> propertyTypesToPopulate ) {

        Set<EntityDataKey> entityDataKeys = ekIds.getEntityKeys( entityKeyIds ).entrySet().stream()
                .map( entry -> new EntityDataKey( entry.getValue().getEntitySetId(), entry.getKey() ) ).collect(
                        Collectors.toSet() );

        Map<EntityDataKey, EntityDataValue> entityValues = entities.getAll( entityDataKeys );

        SetMultimap<UUID, Object> mergedEntity = HashMultimap.create();

        entityValues.entrySet().forEach( entry -> {
            UUID entitySetId = entry.getKey().getEntitySetId();
            Set<UUID> authorizedPropertyTypes = Sets
                    .intersection( propertyTypeIdsByEntitySet.get( entitySetId ), propertyTypesToPopulate );
            mergedEntity.putAll( HazelcastEntityDatastore
                    .fromEntityDataValue( entry.getValue(), authorizedPropertyTypes ) );
        } );

        return mergedEntity;
    }

    @Async
    public void mergeEntity(
            Set<UUID> entityKeyIds,
            UUID graphId,
            UUID syncId,
            Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet,
            Set<UUID> propertyTypesToPopulate,
            Map<UUID, EdmPrimitiveTypeKind> propertyTypesWithDatatype ) {
        SetMultimap<UUID, Object> mergedEntity = computeMergedEntity( entityKeyIds,
                propertyTypeIdsByEntitySet,
                propertyTypesToPopulate );

        String entityId = UUID.randomUUID().toString();

        // create merged entity, in particular get back the entity key id for the new entity
        UUID mergedEntityKeyId;
        try {
            mergedEntityKeyId = createEntity( entityId, mergedEntity, graphId, propertyTypesWithDatatype );

            // write to a lookup table from old entity key id to new, merged entity key id
            entityKeyIds.forEach( oldId -> newIds.put( new LinkingVertexKey( graphId, oldId ), mergedEntityKeyId ) );

        } catch ( ExecutionException | InterruptedException e ) {
            logger.error( "Failed to create linked entity" );
        }

        hazelcastInstance.getCountDownLatch( graphId.toString() ).countDown();
    }

    public UUID createEntity(
            String entityId,
            SetMultimap<UUID, Object> entityDetails,
            UUID graphId,
            Map<UUID, EdmPrimitiveTypeKind> propertyTypesWithDatatype )
            throws ExecutionException, InterruptedException {

        final EntityKey key = new EntityKey( graphId, entityId );
        final ListenableFuture reservationAndVertex = new ListenableHazelcastFuture<>( ids.getAsync( key ) );
        final Stream<ListenableFuture> writes = createDataAsync( entityId,
                graphId,
                entityDetails,
                propertyTypesWithDatatype );
        Stream.concat( Stream.of( reservationAndVertex ), writes ).forEach( DataGraphService::tryGetAndLogErrors );
        return ids.get( key );
    }

    private Stream<ListenableFuture> createDataAsync(
            String entityId,
            UUID graphId,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> propertyTypesWithDatatype ) {

        Set<UUID> authorizedProperties = propertyTypesWithDatatype.keySet();
        // does not write the row if some property values that user is trying to write to are not authorized.
        if ( !authorizedProperties.containsAll( entityDetails.keySet() ) ) {
            String msg = String
                    .format( "Entity %s not written because the following properties are not authorized: %s",
                            entityId,
                            Sets.difference( entityDetails.keySet(), authorizedProperties ) );
            logger.error( msg );
            throw new ForbiddenException( msg );
        }

        EntityKey ek = new EntityKey( graphId, entityId );
        UUID id = ids.get( ek );
        EntityDataKey edk = new EntityDataKey( graphId, id );
        EntityDataUpserter entityDataUpserter =
                new EntityDataUpserter( entityDetails, OffsetDateTime.now() );

        Stream<ListenableFuture> futures = Stream
                .of( new ListenableHazelcastFuture( entities.submitToKey( edk, entityDataUpserter ) ) );

        propertyTypesWithDatatype.entrySet().forEach( entry -> {
            if ( entry.getValue().equals( EdmPrimitiveTypeKind.Binary ) ) {
                entityDetails.removeAll( entry.getKey() );
            }
        } );

        elasticsearchApi.updateEntityData( new EntityDataKey( graphId, id ),
                entityDetails );

        return futures;
    }

    public UUID getMergedId( UUID graphId, UUID oldId ) {
        return Util.getSafely( newIds, new LinkingVertexKey( graphId, oldId ) );
    }

    @Async
    public void mergeEdgeAsync( UUID linkedEntitySetId, Edge edge ) {
        UUID srcEntitySetId = edge.getSrc().getEntitySetId();
        UUID dstEntitySetId = edge.getDst().getEntitySetId();
        UUID edgeEntitySetId = edge.getEdge().getEntitySetId();

        UUID srcId = edge.getKey().getSrc().getEntityKeyId();
        UUID dstId = edge.getKey().getDst().getEntityKeyId();
        UUID edgeId = edge.getKey().getEdge().getEntityKeyId();

        UUID newSrcId = getMergedId( linkedEntitySetId, srcId );
        if ( newSrcId != null ) {
            srcEntitySetId = linkedEntitySetId;
            srcId = newSrcId;
        }
        UUID newDstId = getMergedId( linkedEntitySetId, dstId );
        if ( newDstId != null ) {
            dstEntitySetId = linkedEntitySetId;
            dstId = newDstId;
        }
        UUID newEdgeId = getMergedId( linkedEntitySetId, edgeId );
        if ( newEdgeId != null ) {
            edgeEntitySetId = linkedEntitySetId;
            edgeId = newEdgeId;
        }

    }
}
