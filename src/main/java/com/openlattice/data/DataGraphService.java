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
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.analysis.requests.TopUtilizerDetails;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.data.integration.Association;
import com.openlattice.data.integration.Entity;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.core.Graph;
import com.openlattice.graph.core.objects.NeighborTripletSet;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.hazelcast.HazelcastMap;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.keyvalue.MultiKey;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the graph representation of information stored in the database.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataGraphService implements DataGraphManager {
    private static final Logger                                   logger     = LoggerFactory
            .getLogger( DataGraphService.class );
    private final        Cache<MultiKey, IncrementableWeightId[]> queryCache = CacheBuilder.newBuilder()
            .maximumSize( 1000 )
            .expireAfterWrite( 30, TimeUnit.SECONDS )
            .build();
    private EventBus                 eventBus;
    private Graph                    lm;
    private EntityKeyIdService       idService;
    private EntityDatastore          eds;
    private IMap<UUID, EntitySet>    entitySets;
    private LoadingCache<UUID, UUID> typeIds;

    public DataGraphService(
            HazelcastInstance hazelcastInstance,
            HazelcastEntityDatastore eds,
            Graph lm,
            EntityKeyIdService ids,
            EventBus eventBus ) {
        this.lm = lm;
        this.idService = ids;
        this.eds = eds;
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
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return eds.getEntitySetData( entitySetId, orderedPropertyNames, authorizedPropertyTypes );
    }

    @Override
    public int deleteEntitySet( UUID entitySetId, Map<UUID, PropertyType> authorizedPropertyTypes ) {
        lm.deleteVerticesInEntitySet( entitySetId );
        return eds.deleteEntitySetData( entitySetId, authorizedPropertyTypes );
    }

    @Override
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            UUID entityKeyId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return eds
                .getEntities( entitySetId, ImmutableSet.of( entityKeyId ), authorizedPropertyTypes )
                .iterator()
                .next();
    }

    @Override public int clearEntitySet(
            UUID entitySetId, Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return 0;
    }

    @Override public int clearEntities(
            UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return 0;
    }

    @Override public int clearAssociations( Set<EdgeKey> key ) {
        return 0;
    }

    //TODO: Return information about delete vertices.
    @Override
    public int deleteEntities(
            UUID entitySetId,
            Set<UUID> entityKeyIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        lm.deleteVertices( entitySetId, entityKeyIds );
        return eds.deleteEntities( entitySetId, entityKeyIds, authorizedPropertyTypes );
    }

    @Override
    public int deleteAssociation( Set<EdgeKey> keys, Map<UUID, PropertyType> authorizedPropertyTypes ) {
        final SetMultimap<UUID, UUID> entitySetsToEntityKeyIds = HashMultimap.create();
        for ( EdgeKey edgeKey : keys ) {
            lm.deleteEdge( edgeKey, authorizedPropertyTypes );
            entitySetsToEntityKeyIds.put( edgeKey.getEdgeEntitySetId(), edgeKey.getEdgeEntityKeyId() );
        }

        return Multimaps.asMap( entitySetsToEntityKeyIds )
                .entrySet()
                .stream()
                .mapToInt( e -> eds.deleteEntities( e.getKey(), e.getValue(), authorizedPropertyTypes ) )
                .sum();
    }

    @Override
    public Map<String, UUID> createEntities(
            UUID entitySetId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {

        final Map<String, UUID> ids = idService.assignEntityKeyIds( entitySetId, entities.keySet() );
        final Map<UUID, SetMultimap<UUID, Object>> identifiedEntities =
                entities.entrySet()
                        .stream()
                        .collect( Collectors.toMap( e -> ids.get( e.getKey() ), Entry::getValue ) );
        eds.createEntities( entitySetId, identifiedEntities, authorizedPropertyTypes );
        return ids;
    }

    @Override public void replaceEntities(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Object>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {

    }

    @Override public void partialReplaceEntities(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Object>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {

    }

    @Override public void replacePropertiesInEntities(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Object>>> replacementProperties,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {

    }

    @Override
    public Map<UUID, Map<String,UUID>> integrateAssociations(
            Set<Association> associations,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertiesByEntitySet ) {

        final Map<UUID, Map<String, SetMultimap<UUID, Object>>> associationEntitiesByEntitySet = new HashMap<>();
        final Set<EntityKey> entityKeys = new HashSet<>( 2 * associations.size() );

        for ( Association association : associations ) {
            final Map<String, SetMultimap<UUID, Object>> associationEntities = associationEntitiesByEntitySet
                    .computeIfAbsent( association.getKey().getEntitySetId(), k -> new HashMap<>() );
            associationEntities.put( association.getKey().getEntityId(), association.getDetails() );
            entityKeys.add( association.getSrc() );
            entityKeys.add( association.getDst() );
        }
        final Map<EntityKey, UUID> entityKeyIds = idService.getEntityKeyIds( entityKeys );

        associationEntitiesByEntitySet
                .forEach( ( entitySetId, entities ) ->
                        createEntities( entitySetId, entities, authorizedPropertiesByEntitySet.get( entitySetId ) )
                                .forEach( ( entityId, entityKeyId ) ->
                                        entityKeyIds.put( new EntityKey( entitySetId, entityId ), entityKeyId ) ) );

        associations
                .parallelStream()
                .map( association -> {
                    UUID edgeId = entityKeyIds.get( association.getKey() );

                    UUID srcId = idService.getEntityKeyId( association.getSrc() );
                    UUID srcTypeId = typeIds.getUnchecked( association.getSrc().getEntitySetId() );
                    UUID srcSetId = association.getSrc().getEntitySetId();
                    UUID dstId = idService.getEntityKeyId( association.getDst() );
                    UUID dstTypeId = typeIds.getUnchecked( association.getDst().getEntitySetId() );
                    UUID dstSetId = association.getDst().getEntitySetId();
                    UUID edgeTypeId = typeIds.getUnchecked( association.getKey().getEntitySetId() );
                    UUID edgeSetId = association.getKey().getEntitySetId();

                    return lm
                            .addEdgeAsync( srcId,
                                    srcTypeId,
                                    srcSetId,
                                    dstId,
                                    dstTypeId,
                                    dstSetId,
                                    edgeId,
                                    edgeTypeId,
                                    edgeSetId );
                } ).forEach( DataGraphService::tryGetAndLogErrors );
        return null;
    }

    @Override
    public IntegrationResults integrateEntitiesAndAssociations(
            Set<Entity> entities,
            Set<Association> associations,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertiesByEntitySetId ) {
        // Map<EntityKey, UUID> idsRegistered = new HashMap<>();

        final Map<UUID, Map<String, SetMultimap<UUID, Object>>> entitiesByEntitySet = new HashMap<>();

        for ( Entity entity : entities ) {
            final Map<String, SetMultimap<UUID, Object>> entitiesToCreate = entitiesByEntitySet
                    .computeIfAbsent( entity.getEntitySetId(), k -> new HashMap<>() );
            entitiesToCreate.put( entity.getEntityId(), entity.getDetails() );
        }

        entitiesByEntitySet
                .forEach( ( entitySetId, entitySet ) ->
                        createEntities( entitySetId,
                                entitySet,
                                authorizedPropertiesByEntitySetId.get( entitySetId ) ) );

        integrateAssociations( associations,authorizedPropertiesByEntitySetId );
        return null;
    }

    @Override
    public Stream<SetMultimap<FullQualifiedName, Object>> getTopUtilizers(
            UUID entitySetId,
            List<TopUtilizerDetails> topUtilizerDetailsList,
            int numResults,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
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
