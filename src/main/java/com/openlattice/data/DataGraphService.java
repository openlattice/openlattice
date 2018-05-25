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

package com.openlattice.data;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.analysis.requests.TopUtilizerDetails;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.data.events.EntityDataCreatedEvent;
import com.openlattice.data.requests.Association;
import com.openlattice.data.requests.Entity;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.datastore.exceptions.ResourceNotFoundException;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.core.Graph;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.hazelcast.HazelcastMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataGraphService implements DataGraphManager {
    private static final Logger logger = LoggerFactory
            .getLogger( DataGraphService.class );
    private final ListeningExecutorService executor;
    private final Cache<MultiKey, IncrementableWeightId[]> queryCache = CacheBuilder.newBuilder()
            .maximumSize( 1000 )
            .expireAfterWrite( 30, TimeUnit.SECONDS )
            .build();
    private EventBus                 eventBus;
    private Graph                    lm;
    private EntityKeyIdService       idService;
    private EntityDatastore          eds;
    // Get entity type id by entity set id, cached.
    // TODO HC: Local caching is needed because this would be called very often, so direct calls to IMap should be
    // minimized. Nonetheless, this certainly should be refactored into EdmService or something.
    private IMap<UUID, EntitySet>    entitySets;
    private LoadingCache<UUID, UUID> typeIds;

    public DataGraphService(
            HazelcastInstance hazelcastInstance,
            HazelcastEntityDatastore eds,
            Graph lm,
            EntityKeyIdService ids,
            ListeningExecutorService executor,
            EventBus eventBus ) {
        this.lm = lm;
        this.idService = ids;
        this.eds = eds;
        this.executor = executor;
        this.eventBus = eventBus;

        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
        this.typeIds = CacheBuilder.newBuilder()
                .maximumSize( 100000 ) // 100K * 16 = 16000K = 16MB
                .build( new CacheLoader<UUID, UUID>() {

                    @Override
                    public UUID load( UUID key ) throws Exception {
                        return entitySets.get( key ).getEntityTypeId();
                    }
                } );
    }

    @Override
    public EntitySetData<FullQualifiedName> getEntitySetData(
            UUID entitySetId,
            LinkedHashSet<String> orderedPropertyNames,
            Set<PropertyType> authorizedPropertyTypes ) {
        return eds.getEntitySetData( entitySetId, orderedPropertyNames, authorizedPropertyTypes );
    }

    @Override
    public int deleteEntitySetData( UUID entitySetId,Set<PropertyType> authorizedPropertyTypes ) {
        return eds.deleteEntitySetData( entitySetId,authorizedPropertyTypes );
        // TODO delete all vertices
    }

    @Override
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entityKeyId, Set<PropertyType> authorizedPropertyTypes ) {
        EntityKey entityKey = idService.getEntityKey( entityKeyId );
        return eds.getEntity( entityKey.getEntitySetId(),
                entityKey.getEntityId(),
                authorizedPropertyTypes );
    }

    @Override
    public void updateEntity(
            UUID id,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        EntityKey elementReference = idService.getEntityKey( id );
        updateEntity( elementReference, entityDetails, authorizedPropertiesWithDataType );
    }

    @Override
    public void updateEntity(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        eds.updateEntity( entityKey, entityDetails, authorizedPropertiesWithDataType );
    }

    @Override
    public void deleteEntity( EntityDataKey edk ) {
        lm.deleteVertex( edk.getEntityKeyId() );
        eds.deleteEntities( edk );
    }

    @Override
    public void deleteAssociation( EdgeKey key ) {
        EntityKey entityKey = idService.getEntityKey( key.getEdgeEntityKeyId() );
        lm.deleteEdge( key );
        eds.deleteEntities( new EntityDataKey( entityKey.getEntitySetId(), key.getEdgeEntityKeyId() ) );
    }

    @Override
    public UUID createEntity(
            UUID entitySetId,
            String entityId,
            SetMultimap<UUID, Object> entityDetails,
            Set<PropertyType> authorizedPropertyTypes )
            throws ExecutionException, InterruptedException {

        final EntityKey key = new EntityKey( entitySetId, entityId );
        createEntity( key, entityDetails, authorizedPropertiesWithDataType )
                .forEach( DataGraphService::tryGetAndLogErrors );
        return idService.getEntityKeyId( key );
    }

    @Override
    public void createEntities(
            UUID entitySetId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws ExecutionException, InterruptedException {
        entities.entrySet()
                .parallelStream()
                .flatMap(
                        entity -> {
                            final EntityKey key = new EntityKey( entitySetId, entity.getKey() );
                            return createEntity( key, entity.getValue(), authorizedPropertiesWithDataType );
                        } )
                .forEach( DataGraphService::tryGetAndLogErrors );
    }

    private Stream<ListenableFuture> createEntity(
            EntityKey key,
            SetMultimap<UUID, Object> details,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        final ListenableFuture reservationAndVertex = idService.getEntityKeyIdAsync( key );
        final Stream<ListenableFuture> writes = eds.updateEntityAsync( key, details, authorizedPropertiesWithDataType );
        return Stream.concat( Stream.of( reservationAndVertex ), writes );
    }

    public void replaceEntity(
            EntityDataKey edk,
            SetMultimap<UUID, Object> entity,
            Map<UUID, EdmPrimitiveTypeKind> propertyTypes ) {
        EntityKey key = idService.getEntityKey( edk.getEntityKeyId() );
        eds.deleteEntities( edk );
        eds.updateEntityAsync( key, entity, propertyTypes ).forEach( DataGraphService::tryGetAndLogErrors );

        propertyTypes.entrySet().forEach( entry -> {
            if ( entry.getValue().equals( EdmPrimitiveTypeKind.Binary ) ) { entity.removeAll( entry.getKey() ); }
        } );

        eventBus.post( new EntityDataCreatedEvent( edk, entity, false ) );
    }

    @Override
    public void createAssociations(
            UUID entitySetId,
            Set<Association> associations,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType )
            throws InterruptedException, ExecutionException {
        associations
                .parallelStream()
                .flatMap( association -> {
                    UUID edgeId = idService.getEntityKeyId( association.getKey() );

                    Stream<ListenableFuture> writes = eds.updateEntityAsync( association.getKey(),
                            association.getDetails(),
                            authorizedPropertiesWithDataType );

                    UUID srcId = idService.getEntityKeyId( association.getSrc() );
                    UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                    UUID srcSetId = association.getSrc().getEntitySetId();
                    UUID dstId = idService.getEntityKeyId( association.getDst() );
                    UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                    UUID dstSetId = association.getDst().getEntitySetId();
                    UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );
                    UUID edgeSetId = association.getKey().getEntitySetId();

                    ListenableFuture addEdge = lm
                            .addEdgeAsync( srcId,
                                    srcTypeId,
                                    srcSetId,
                                    dstId,
                                    dstTypeId,
                                    dstSetId,
                                    edgeId,
                                    edgeTypeId,
                                    edgeSetId );
                    return Stream.concat( writes, Stream.of( addEdge ) );
                } ).forEach( DataGraphService::tryGetAndLogErrors );
    }

    @Override
    public void createEntitiesAndAssociations(
            Set<Entity> entities,
            Set<Association> associations,
            Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId ) {
        // Map<EntityKey, UUID> idsRegistered = new HashMap<>();

        entities.parallelStream()
                .flatMap( entity -> createEntity( entity.getKey(),
                        entity.getDetails(),
                        authorizedPropertiesByEntitySetId.get( entity.getKey().getEntitySetId() ) ) )
                .forEach( DataGraphService::tryGetAndLogErrors );

        associations.parallelStream().flatMap( association -> {
            UUID srcId = idService.getEntityKeyId( association.getSrc() );
            UUID dstId = idService.getEntityKeyId( association.getDst() );
            if ( srcId == null || dstId == null ) {
                String err = String.format(
                        "Edge %s cannot be created because some vertices failed to register for an id.",
                        association.toString() );
                logger.debug( err );
                return Stream.of( Futures.immediateFailedFuture( new ResourceNotFoundException( err ) ) );
            } else {
                Stream<ListenableFuture> writes = eds.updateEntityAsync( association.getKey(),
                        association.getDetails(),
                        authorizedPropertiesByEntitySetId.get( association.getKey().getEntitySetId() ) );

                UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                UUID srcSetId = association.getSrc().getEntitySetId();
                UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                UUID dstSetId = association.getDst().getEntitySetId();
                UUID edgeId = idService.getEntityKeyId( association.getKey() );
                UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );
                UUID edgeSetId = association.getKey().getEntitySetId();

                ListenableFuture addEdge = lm.addEdgeAsync( srcId,
                        srcTypeId,
                        srcSetId,
                        dstId,
                        dstTypeId,
                        dstSetId,
                        edgeId,
                        edgeTypeId,
                        edgeSetId );
                return Stream.concat( writes, Stream.of( addEdge ) );
            }
        } ).forEach( DataGraphService::tryGetAndLogErrors );
    }

    @Override
    public Stream<SetMultimap<FullQualifiedName, Object>> getTopUtilizers(
            UUID entitySetId,
            List<TopUtilizerDetails> topUtilizerDetailsList,
            int numResults,
            Set<PropertyType> authorizedPropertyTypes )
            throws InterruptedException, ExecutionException {
        /*
         * ByteBuffer queryId; try { queryId = ByteBuffer.wrap( ObjectMappers.getSmileMapper().writeValueAsBytes(
         * topUtilizerDetailsList ) ); } catch ( JsonProcessingException e1 ) { logger.debug(
         * "Unable to generate query id." ); return null; }
         */
        IncrementableWeightId[] maybeUtilizers = queryCache
                .getIfPresent( new MultiKey( entitySetId, topUtilizerDetailsList ) );
        final IncrementableWeightId[] utilizers;
        // if ( !eds.queryAlreadyExecuted( queryId ) ) {
        if ( maybeUtilizers == null ) {
            //            utilizers = new TopUtilizers( numResults );
            SetMultimap<UUID, UUID> srcFilters = HashMultimap.create();
            SetMultimap<UUID, UUID> dstFilters = HashMultimap.create();

            topUtilizerDetailsList.forEach( details -> {
                ( details.getUtilizerIsSrc() ? srcFilters : dstFilters ).
                        putAll( details.getAssociationTypeId(), details.getNeighborTypeIds() );

            } );
            utilizers = lm.computeGraphAggregation( numResults, entitySetId, srcFilters, dstFilters );

            queryCache.put( new MultiKey( entitySetId, topUtilizerDetailsList ), utilizers );
        } else {
            utilizers = maybeUtilizers;
        }
        //TODO: this returns unsorted data.
        final UUID[] utilizerIds = new UUID[ utilizers.length ];
        for ( int i = 0; i < utilizers.length; ++i ) {
            utilizerIds[ i ] = utilizers[ i ].getId();
        }
        return eds.getEntities( entitySetId, ImmutableSet.copyOf( utilizerIds ), authorizedPropertyTypes );
    }

    @Override
    public NeighborTripletSet getNeighborEntitySets( UUID entitySetId ) {
        return lm.getNeighborEntitySets( entitySetId );
    }

    public static void tryGetAndLogErrors( ListenableFuture<?> f ) {
        try {
            f.get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Future execution failed.", e );
        }
    }
}
