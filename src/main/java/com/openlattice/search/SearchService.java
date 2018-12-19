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

package com.openlattice.search;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.openlattice.apps.App;
import com.openlattice.apps.AppType;
import com.openlattice.authorization.AbstractSecurableObjectResolveTypeService;
import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDatastore;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.events.EntitiesDeletedEvent;
import com.openlattice.data.events.EntitiesUpsertedEvent;
import com.openlattice.data.events.EntityDataCreatedEvent;
import com.openlattice.data.events.EntityDataDeletedEvent;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.data.storage.PostgresDataManager;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.events.*;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.core.GraphService;
import com.openlattice.graph.edge.Edge;
import com.openlattice.neuron.audit.AuditEntitySetUtils;
import com.openlattice.organization.Organization;
import com.openlattice.organizations.events.OrganizationCreatedEvent;
import com.openlattice.organizations.events.OrganizationDeletedEvent;
import com.openlattice.organizations.events.OrganizationUpdatedEvent;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.openlattice.postgres.DataTables.ID_FQN;

public class SearchService {
    private static final Logger logger = LoggerFactory.getLogger( SearchService.class );

    @Inject
    private EventBus eventBus;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    @Inject
    private EdmManager dataModelService;

    @Inject
    private GraphService graphService;

    @Inject
    private EntityDatastore dataManager;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Inject
    private EntityKeyIdService entityKeyService;

    @Inject
    private PostgresDataManager postgresDataManager;

    public SearchService( EventBus eventBus ) {
        eventBus.register( this );
    }

    @Timed
    public SearchResult executeEntitySetKeywordSearchQuery(
            Optional<String> optionalQuery,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            int start,
            int maxHits ) {

        Set<AclKey> authorizedEntitySetIds = authorizations
                .getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                        SecurableObjectType.EntitySet,
                        EnumSet.of( Permission.READ ) ).collect( Collectors.toSet() );
        if ( authorizedEntitySetIds.size() == 0 ) { return new SearchResult( 0, Lists.newArrayList() ); }

        return elasticsearchApi.executeEntitySetMetadataSearch(
                optionalQuery,
                optionalEntityType,
                optionalPropertyTypes,
                authorizedEntitySetIds,
                start,
                maxHits );
    }

    @Timed
    public DataSearchResult executeSearch(
            SearchConstraints searchConstraints,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet ) {
        Map<UUID, DelegatedUUIDSet> authorizedPropertiesByEntitySet = authorizedPropertyTypesByEntitySet
                .entrySet().stream()
                .collect( Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> DelegatedUUIDSet.wrap( Sets.newHashSet( entry.getValue().keySet() ) ) )
                );

        EntityDataKeySearchResult result = elasticsearchApi
                .executeSearch( searchConstraints, authorizedPropertiesByEntitySet );

        SetMultimap<UUID, UUID> entityKeyIdsByEntitySetId = HashMultimap.create();
        result.getEntityDataKeys()
                .forEach( edk -> entityKeyIdsByEntitySetId.put( edk.getEntitySetId(), edk.getEntityKeyId() ) );

        List<SetMultimap<FullQualifiedName, Object>> results = entityKeyIdsByEntitySetId.keySet().parallelStream()
                .map( entitySetId -> getResults(
                        entitySetId,
                        entityKeyIdsByEntitySetId.get( entitySetId ),
                        authorizedPropertyTypesByEntitySet ) )
                .flatMap( entityList -> entityList.stream() )
                .collect( Collectors.toList() );

        return new DataSearchResult( result.getNumHits(), results );
    }


    @Timed
    public DataSearchResult executeLinkingSearch(
            SearchConstraints searchConstraints,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet ) {
        Map<UUID, DelegatedUUIDSet> authorizedPropertiesByEntitySet = authorizedPropertyTypesByEntitySet
                .entrySet().stream()
                .collect( Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> DelegatedUUIDSet.wrap( Sets.newHashSet( entry.getValue().keySet() ) ) )
                );
        // Always have to load all entities to ensure, all linked entity data is included
        int hitNum = 0;
        long totalHits;
        final int start = searchConstraints.getStart();
        final int maxHits = searchConstraints.getMaxHits();

        Map<UUID, List<Map<FullQualifiedName, Set<Object>>>> entityDataByLinkingId = new HashMap<>();

        do {
            SearchConstraints remainingSearchConstraint = new SearchConstraints(
                    searchConstraints.getEntitySetIds(),
                    hitNum,
                    SearchApi.MAX_SEARCH_RESULTS,
                    searchConstraints.getConstraintGroups() );

            EntityDataKeySearchResult result = elasticsearchApi
                    .executeSearch( remainingSearchConstraint, authorizedPropertiesByEntitySet );
            totalHits = result.getNumHits();

            if( !result.getEntityDataKeys().isEmpty() ) {
                SetMultimap<UUID, UUID> entityKeyIdsByEntitySetId = HashMultimap.create();
                Set<UUID> entityKeyIds = Sets.newHashSet();
                result.getEntityDataKeys()
                        .forEach( edk -> {
                            entityKeyIdsByEntitySetId.put( edk.getEntitySetId(), edk.getEntityKeyId() );
                            entityKeyIds.add( edk.getEntityKeyId() );
                        } );

                Map<UUID, Map<FullQualifiedName, Set<Object>>> results = entityKeyIdsByEntitySetId.keySet().parallelStream()
                        .map( entitySetId -> getResultsById(
                                entitySetId,
                                entityKeyIdsByEntitySetId.get( entitySetId ),
                                authorizedPropertyTypesByEntitySet ) )
                        .map( it -> it.entrySet() )
                        .flatMap( it -> it.stream() )
                        .collect( Collectors.toMap( it -> it.getKey(), it -> it.getValue() ) );

                PostgresIterable<Pair<UUID, UUID>> linkingIdsByEntityKeyIds = getLinkingIdsByEntityKeyIds( entityKeyIds );

                linkingIdsByEntityKeyIds.stream()
                        .forEach( ids -> {
                            entityDataByLinkingId.merge(
                                    ids.getValue(), // linking_id
                                    List.of( results.get( ids.getKey() ) ), // entity_key_id
                                    ( list1, list2 ) -> Stream.of( list1, list2 )
                                            .flatMap( Collection::stream )
                                            .collect( Collectors.toList() ) );
                        } );

                hitNum += entityKeyIds.size();
            }
        } while ( hitNum < totalHits );

        if( entityDataByLinkingId.size() >= start ) {
            List<SetMultimap<FullQualifiedName, Object>> mergedEntityDataByLinkingId =
                    mergeEntityDataByLinkingId( entityDataByLinkingId ).stream()
                            .skip( start )
                            .limit( maxHits )
                            .collect( Collectors.toList());
            return new DataSearchResult( totalHits, mergedEntityDataByLinkingId );

        } else {
            return new DataSearchResult( totalHits, Lists.newArrayList() );
        }

    }

    private List<SetMultimap<FullQualifiedName, Object>> mergeEntityDataByLinkingId(
            Map<UUID, List<Map<FullQualifiedName, Set<Object>>>> entityDataOfLinkingIds ) {

        return entityDataOfLinkingIds.entrySet().stream().map( linkedEntityDataList -> {
            SetMultimap<FullQualifiedName, Object> mergedEntityDataMultimap = HashMultimap.create();
            Map<FullQualifiedName, Set<Object>> mergedEntityData = linkedEntityDataList.getValue().stream()
                    .reduce( ( dataMap, nextDataMap ) -> {
                        nextDataMap
                                .forEach( ( fqn, dataSet ) ->
                                        dataMap.merge( fqn, dataSet, ( dataSet1, dataSet2 ) -> Stream.of( dataSet1, dataSet2 )
                                                .flatMap( Collection::stream )
                                                .collect( Collectors.toSet() ) ) );
                        return dataMap; } ).get();
            mergedEntityData.put( ID_FQN, ImmutableSet.of( linkedEntityDataList.getKey() ) );
            mergedEntityData.forEach(mergedEntityDataMultimap::putAll);
            return mergedEntityDataMultimap;
        } ).collect( Collectors.toList() );
    }

    @Timed
    @Subscribe
    public void deleteEntitySet( EntitySetDeletedEvent event ) {
        elasticsearchApi.deleteEntitySet( event.getEntitySetId() );
    }

    @Timed
    @Subscribe
    public void deleteEntities( EntitiesDeletedEvent event ) {
        event.getEntityKeyIds()
                .stream()
                .map( id -> new EntityDataKey( event.getEntitySetId(), id ) )
                .forEach( elasticsearchApi::deleteEntityData );
    }

    @Timed
    @Subscribe
    public void entitySetDataCleared( EntitySetDataClearedEvent event ) {
        elasticsearchApi.clearEntitySetData( event.getEntitySetId() );
    }

    @Timed
    @Subscribe
    public void createOrganization( OrganizationCreatedEvent event ) {
        elasticsearchApi.createOrganization( event.getOrganization() );
    }

    @Timed
    public SearchResult executeOrganizationKeywordSearch( SearchTerm searchTerm ) {
        Set<AclKey> authorizedOrganizationIds = authorizations
                .getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                        SecurableObjectType.Organization,
                        EnumSet.of( Permission.READ ) ).collect( Collectors.toSet() );
        if ( authorizedOrganizationIds.size() == 0 ) { return new SearchResult( 0, Lists.newArrayList() ); }

        return elasticsearchApi.executeOrganizationSearch( searchTerm.getSearchTerm(),
                authorizedOrganizationIds,
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @Timed
    @Subscribe
    public void updateOrganization( OrganizationUpdatedEvent event ) {
        elasticsearchApi.updateOrganization( event.getId(),
                event.getOptionalTitle(),
                event.getOptionalDescription() );
    }

    @Subscribe
    public void deleteOrganization( OrganizationDeletedEvent event ) {
        elasticsearchApi.deleteOrganization( event.getOrganizationId() );
    }

    @Subscribe
    public void indexEntities( EntitiesUpsertedEvent event ) {
        if ( event.isUpdate() ) {
            event.getEntities()
                    .forEach( ( entitKeyId, entity ) -> elasticsearchApi
                            .updateEntityData( new EntityDataKey( event.getEntitySetId(), entitKeyId ), entity ) );
        } else {
            event.getEntities()
                    .forEach( ( entitKeyId, entity ) -> elasticsearchApi
                            .createEntityData( new EntityDataKey( event.getEntitySetId(), entitKeyId ), entity ) );
        }
    }

    @Subscribe
    public void createEntityData( EntityDataCreatedEvent event ) {
        EntityDataKey edk = event.getEntityDataKey();
        Map<UUID, Set<Object>> entity = event.getPropertyValues();
        if ( event.getShouldUpdate() ) {
            elasticsearchApi.updateEntityData( edk, entity );
        } else {
            elasticsearchApi.createEntityData( edk, entity );
        }
    }

    @Subscribe
    public void deleteEntityData( EntityDataDeletedEvent event ) {
        EntityDataKey edk = event.getEntityDataKey();
        elasticsearchApi.deleteEntityData( edk );
    }

    @Subscribe
    public void updateEntitySetMetadata( EntitySetMetadataUpdatedEvent event ) {
        elasticsearchApi.updateEntitySetMetadata( event.getEntitySet() );
    }

    @Subscribe
    public void updatePropertyTypesInEntitySet( PropertyTypesInEntitySetUpdatedEvent event ) {
        elasticsearchApi.updatePropertyTypesInEntitySet( event.getEntitySetId(),
                event.getNewPropertyTypes() );
    }

    @Timed
    public List<UUID> executeEntitySetDataSearchAcrossIndices(
            Iterable<UUID> entitySetIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        // TODO linking entity sets, if we ever use it that way
        return elasticsearchApi.executeEntitySetDataSearchAcrossIndices( entitySetIds,
                fieldSearches,
                size,
                explain );
    }

    @Subscribe
    public void createEntityType( EntityTypeCreatedEvent event ) {
        EntityType entityType = event.getEntityType();
        elasticsearchApi.saveEntityTypeToElasticsearch( entityType );
    }

    @Subscribe
    public void createAssociationType( AssociationTypeCreatedEvent event ) {
        AssociationType associationType = event.getAssociationType();
        elasticsearchApi.saveAssociationTypeToElasticsearch( associationType );
    }

    @Subscribe
    public void createApp( AppCreatedEvent event ) {
        App app = event.getApp();
        elasticsearchApi.saveAppToElasticsearch( app );
    }

    @Subscribe
    public void createAppType( AppTypeCreatedEvent event ) {
        AppType appType = event.getAppType();
        elasticsearchApi.saveAppTypeToElasticsearch( appType );
    }

    @Subscribe
    public void deleteEntityType( EntityTypeDeletedEvent event ) {
        UUID entityTypeId = event.getEntityTypeId();
        elasticsearchApi.deleteEntityType( entityTypeId );
    }

    @Subscribe
    public void deleteAssociationType( AssociationTypeDeletedEvent event ) {
        UUID associationTypeId = event.getAssociationTypeId();
        elasticsearchApi.deleteAssociationType( associationTypeId );
    }

    @Subscribe
    public void deletePropertyType( PropertyTypeDeletedEvent event ) {
        UUID propertyTypeId = event.getPropertyTypeId();
        elasticsearchApi.deletePropertyType( propertyTypeId );
    }

    @Subscribe
    public void deleteApp( AppDeletedEvent event ) {
        UUID appId = event.getAppId();
        elasticsearchApi.deleteApp( appId );
    }

    @Subscribe
    public void deleteAppType( AppTypeDeletedEvent event ) {
        UUID appTypeId = event.getAppTypeId();
        elasticsearchApi.deleteAppType( appTypeId );
    }

    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeEntityTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeAssociationTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executePropertyTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeAppSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeAppTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi.executeFQNEntityTypeSearch( namespace, name, start, maxHits );
    }

    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi.executeFQNPropertyTypeSearch( namespace, name, start, maxHits );
    }

    @Timed
    public Map<UUID, List<NeighborEntityDetails>> executeLinkingEntityNeighborSearch(
            Set<UUID> linkedEntitySetIds,
            EntityNeighborsFilter filter ) {
        if ( filter.getAssociationEntitySetIds().isPresent() && filter.getAssociationEntitySetIds().get().isEmpty() ) {
            return ImmutableMap.of();
        }
        
        Set<UUID> linkingIds = filter.getEntityKeyIds();

        PostgresIterable<Pair<UUID, Set<UUID>>> entityKeyIdsByLinkingIds = getEntityKeyIdsByLinkingIds( linkingIds );

        Set<UUID> entityKeyIds = entityKeyIdsByLinkingIds.stream()
                .flatMap( entityKeyIdsOfLinkingId -> entityKeyIdsOfLinkingId.getRight().stream() )
                .collect( Collectors.toSet() );

        // Will return only entries, where there is at least 1 neighbor
        Map<UUID, List<NeighborEntityDetails>> entityNeighbors = executeEntityNeighborSearch(
                linkedEntitySetIds,
                new EntityNeighborsFilter( entityKeyIds,
                        filter.getSrcEntitySetIds(),
                        filter.getDstEntitySetIds(),
                        filter.getAssociationEntitySetIds() ) );

        if ( entityNeighbors.isEmpty() ) {
            return entityNeighbors;
        }

        return entityKeyIdsByLinkingIds.stream()
                .filter( entityKeyIdsOfLinkingId ->
                        entityNeighbors.keySet().stream().anyMatch( entityKeyIdsOfLinkingId.getRight()::contains ) )
                .collect( Collectors.toMap(
                        Pair::getLeft, // linking_id
                        entityKeyIdsOfLinkingId -> {
                            ImmutableList.Builder<NeighborEntityDetails> linkedNeighbours = ImmutableList.builder();
                            entityKeyIdsOfLinkingId.getRight().stream()
                                    .filter( entityKeyId -> entityNeighbors.containsKey( entityKeyId ) )
                                    .forEach( entityKeyId -> linkedNeighbours.addAll( entityNeighbors.get( entityKeyId ) ) );
                            return linkedNeighbours.build();
                        }
                ) );
    }

    @Timed
    public Map<UUID, List<NeighborEntityDetails>> executeEntityNeighborSearch(
            Set<UUID> entitySetIds,
            EntityNeighborsFilter filter ) {
        if ( filter.getAssociationEntitySetIds().isPresent() && filter.getAssociationEntitySetIds().get().isEmpty() ) {
            return ImmutableMap.of();
        }

        Set<Principal> principals = Principals.getCurrentPrincipals();

        Set<UUID> entityKeyIds = filter.getEntityKeyIds();

        List<Edge> edges = Lists.newArrayList();
        Set<UUID> allEntitySetIds = Sets.newHashSet();
        Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds = Maps.newHashMap();
        SetMultimap<UUID, UUID> entitySetIdToEntityKeyId = HashMultimap.create();
        Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps = Maps.newHashMap();

        graphService.getEdgesAndNeighborsForVerticesBulk( entitySetIds, filter ).forEach( edge -> {
            edges.add( edge );
            allEntitySetIds.add( edge.getEdge().getEntitySetId() );
            allEntitySetIds.add( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ?
                    edge.getDst().getEntitySetId() : edge.getSrc().getEntitySetId() );
        } );

        Set<UUID> authorizedEntitySetIds = authorizations.accessChecksForPrincipals( allEntitySetIds.stream()
                .map( esId -> new AccessCheck( new AclKey( esId ), EnumSet.of( Permission.READ ) ) )
                .collect( Collectors.toSet() ), principals )
                .filter( auth -> auth.getPermissions().get( Permission.READ ) ).map( auth -> auth.getAclKey().get( 0 ) )
                .collect( Collectors.toSet() );

        Map<UUID, EntitySet> entitySetsById = dataModelService.getEntitySetsAsMap( authorizedEntitySetIds );

        Map<UUID, EntityType> entityTypesById = dataModelService
                .getEntityTypesAsMap( entitySetsById.values().stream().map( entitySet -> {
                    entitySetsIdsToAuthorizedProps.put( entitySet.getId(), Maps.newHashMap() );
                    authorizedEdgeESIdsToVertexESIds.put( entitySet.getId(), Sets.newHashSet() );
                    return entitySet.getEntityTypeId();
                } ).collect( Collectors.toSet() ) );

        Map<UUID, PropertyType> propertyTypesById = dataModelService
                .getPropertyTypesAsMap( entityTypesById.values().stream()
                        .flatMap( entityType -> entityType.getProperties().stream() ).collect(
                                Collectors.toSet() ) );

        Set<AccessCheck> accessChecks = entitySetsById.values().stream()
                .flatMap( entitySet -> entityTypesById.get( entitySet.getEntityTypeId() ).getProperties().stream()
                        .map( propertyTypeId -> new AccessCheck( new AclKey( entitySet.getId(), propertyTypeId ),
                                EnumSet.of( Permission.READ ) ) ) ).collect( Collectors.toSet() );

        authorizations.accessChecksForPrincipals( accessChecks, principals ).forEach( auth -> {
            if ( auth.getPermissions().get( Permission.READ ) ) {
                UUID esId = auth.getAclKey().get( 0 );
                UUID propertyTypeId = auth.getAclKey().get( 1 );
                entitySetsIdsToAuthorizedProps.get( esId )
                        .put( propertyTypeId, propertyTypesById.get( propertyTypeId ) );
            }
        } );

        edges.forEach( edge -> {
            UUID edgeEntityKeyId = edge.getEdge().getEntityKeyId();
            UUID neighborEntityKeyId = ( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ) ? edge.getDst()
                    .getEntityKeyId()
                    : edge.getSrc().getEntityKeyId();
            UUID edgeEntitySetId = edge.getEdge().getEntitySetId();
            UUID neighborEntitySetId = ( entityKeyIds.contains( edge.getSrc().getEntityKeyId() ) ) ? edge.getDst()
                    .getEntitySetId()
                    : edge.getSrc().getEntitySetId();

            if ( entitySetsIdsToAuthorizedProps.containsKey( edgeEntitySetId ) ) {
                entitySetIdToEntityKeyId.put( edgeEntitySetId, edgeEntityKeyId );

                if ( entitySetsIdsToAuthorizedProps.containsKey( neighborEntitySetId ) ) {
                    authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId ).add( neighborEntitySetId );
                    entitySetIdToEntityKeyId.put( neighborEntitySetId, neighborEntityKeyId );
                }
            }

        } );

        ListMultimap<UUID, SetMultimap<FullQualifiedName, Object>> entitiesByEntitySetId = dataManager
                .getEntitiesAcrossEntitySets( entitySetIdToEntityKeyId, entitySetsIdsToAuthorizedProps );

        Map<UUID, SetMultimap<FullQualifiedName, Object>> entities = Maps.newHashMap();
        entitiesByEntitySetId.entries().forEach( entry -> entities
                .put( UUID.fromString( entry.getValue().get( new FullQualifiedName( "openlattice.@id" ) ).iterator()
                                .next().toString() ),
                        entry.getValue() ) );

        Map<UUID, List<NeighborEntityDetails>> entityNeighbors = Maps.newConcurrentMap();

        // create a NeighborEntityDetails object for each edge based on authorizations
        edges.parallelStream().forEach( edge -> {
            boolean vertexIsSrc = entityKeyIds.contains( edge.getKey().getSrc().getEntityKeyId() );
            UUID entityId = ( vertexIsSrc )
                    ? edge.getKey().getSrc().getEntityKeyId()
                    : edge.getKey().getDst().getEntityKeyId();
            if ( !entityNeighbors.containsKey( entityId ) ) {
                entityNeighbors.put( entityId, Collections.synchronizedList( Lists.newArrayList() ) );
            }
            NeighborEntityDetails neighbor = getNeighborEntityDetails( edge,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsById,
                    vertexIsSrc,
                    entities );
            if ( neighbor != null ) {
                entityNeighbors.get( entityId ).add( neighbor );
            }
        } );

        return entityNeighbors;

    }

    private boolean getAuthorization( UUID entitySetId, Set<Principal> principals ) {
        return authorizations.accessChecksForPrincipals( ImmutableSet
                .of( new AccessCheck( new AclKey( entitySetId ), EnumSet.of( Permission.READ ) ) ), principals )
                .findFirst().get().getPermissions().get( Permission.READ );
    }

    private Map<UUID, PropertyType> getAuthorizedProperties( UUID entitySetId ) {
        return authzHelper
                .getAuthorizedPropertiesOnEntitySet( entitySetId,
                        EnumSet.of( Permission.READ ) )
                .stream()
                .collect( Collectors.toMap( ptId -> ptId,
                        ptId -> dataModelService.getPropertyType( ptId ) ) );
    }

    private NeighborEntityDetails getNeighborEntityDetails(
            Edge edge,
            Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds,
            Map<UUID, EntitySet> entitySetsById,
            boolean vertexIsSrc,
            Map<UUID, SetMultimap<FullQualifiedName, Object>> entities ) {

        UUID edgeEntitySetId = edge.getEdge().getEntitySetId();
        if ( authorizedEdgeESIdsToVertexESIds.containsKey( edgeEntitySetId ) ) {
            UUID neighborEntityKeyId = ( vertexIsSrc )
                    ? edge.getDst().getEntityKeyId()
                    : edge.getSrc().getEntityKeyId();
            UUID neighborEntitySetId = ( vertexIsSrc )
                    ? edge.getDst().getEntitySetId()
                    : edge.getSrc().getEntitySetId();

            SetMultimap<FullQualifiedName, Object> edgeDetails = entities.get( edge.getEdge().getEntityKeyId() );
            if ( edgeDetails != null ) {
                if ( authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId )
                        .contains( neighborEntitySetId ) ) {
                    SetMultimap<FullQualifiedName, Object> neighborDetails = entities.get( neighborEntityKeyId );

                    if ( neighborDetails != null ) {
                        return new NeighborEntityDetails(
                                entitySetsById.get( edgeEntitySetId ),
                                edgeDetails,
                                entitySetsById.get( neighborEntitySetId ),
                                neighborEntityKeyId,
                                neighborDetails,
                                vertexIsSrc );
                    }

                } else {
                    return new NeighborEntityDetails(
                            entitySetsById.get( edgeEntitySetId ),
                            edgeDetails,
                            vertexIsSrc );
                }
            }
        }
        return null;
    }

    private List<SetMultimap<FullQualifiedName, Object>> getResults(
            UUID entitySetId,
            Set<UUID> entityKeyIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes ) {
        if ( entityKeyIds.size() == 0 ) { return ImmutableList.of(); }
        return dataManager
                .getEntities( entitySetId, ImmutableSet.copyOf( entityKeyIds ), authorizedPropertyTypes )
                .collect( Collectors.toList() );
    }

    private Map<UUID, Map<FullQualifiedName, Set<Object>>> getResultsById(
            UUID entitySetId,
            Set<UUID> entityKeyIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes ) {
        if ( entityKeyIds.size() == 0 ) { return Map.of(); }
        return dataManager
                .getEntitiesById( entitySetId, ImmutableSet.copyOf( entityKeyIds ), authorizedPropertyTypes )
                .stream().collect( Collectors.toMap(
                        Pair::getLeft,
                        Pair::getRight ) );
    }

    private PostgresIterable<Pair<UUID, UUID>> getLinkingIdsByEntityKeyIds(
            Set<UUID> entityKeyIds ) {
        return dataManager.getLinkingIds( entityKeyIds );
    }

    private PostgresIterable<Pair<UUID, Set<UUID>>> getEntityKeyIdsByLinkingIds(
            Set<UUID> linkingIds ) {
        return dataManager.getEntityKeyIdsOfLinkingIds( linkingIds );
    }

    @Subscribe
    public void clearAllData( ClearAllDataEvent event ) {
        elasticsearchApi.clearAllData();
    }

    public void triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        elasticsearchApi.triggerPropertyTypeIndex( propertyTypes );
    }

    public void triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        elasticsearchApi.triggerEntityTypeIndex( entityTypes );
    }

    public void triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        elasticsearchApi.triggerAssociationTypeIndex( associationTypes );
    }

    public void triggerEntitySetIndex() {
        Map<EntitySet, Set<UUID>> entitySets = StreamUtil.stream( dataModelService.getEntitySets() ).collect( Collectors
                .toMap( entitySet -> entitySet,
                        entitySet -> dataModelService.getEntityType( entitySet.getEntityTypeId() ).getProperties() ) );
        Map<UUID, PropertyType> propertyTypes = StreamUtil.stream( dataModelService.getPropertyTypes() )
                .collect( Collectors.toMap( pt -> pt.getId(), pt -> pt ) );
        elasticsearchApi.triggerEntitySetIndex( entitySets, propertyTypes );
    }

    public void triggerEntitySetDataIndex( UUID entitySetId ) {
        Set<UUID> propertyTypeIds = dataModelService.getEntityTypeByEntitySetId( entitySetId ).getProperties();
        Map<UUID, PropertyType> propertyTypes = dataModelService.getPropertyTypesAsMap( propertyTypeIds );
        List<PropertyType> propertyTypeList = Lists.newArrayList( propertyTypes.values() );

        elasticsearchApi.deleteEntitySet( entitySetId );
        elasticsearchApi.saveEntitySetToElasticsearch( dataModelService.getEntitySet( entitySetId ), propertyTypeList );

        Set<PropertyType> propertyTypesToLoad = propertyTypeList.stream()
                .filter( pt -> !pt.getDatatype().equals( EdmPrimitiveTypeKind.Binary ) ).collect(
                        Collectors.toSet() );

        postgresDataManager.getEntitiesInEntitySet( entitySetId, propertyTypesToLoad )
                .parallel()
                .forEach( entity -> {
                    EntityDataKey edk = new EntityDataKey( entitySetId, entity.getEntityKeyId() );
                    Map<UUID, Set<Object>> values = entity.getProperties();
                    elasticsearchApi.createEntityData( edk, values );
                } );
    }

    public void triggerAllEntitySetDataIndex() {
        dataModelService.getEntitySets().forEach( entitySet -> {
            if ( !entitySet.getName().equals( AuditEntitySetUtils.AUDIT_ENTITY_SET_NAME ) ) {
                triggerEntitySetDataIndex( entitySet.getId() );
            }
        } );
    }

    public void triggerAppIndex( List<App> apps ) {
        elasticsearchApi.triggerAppIndex( apps );
    }

    public void triggerAppTypeIndex( List<AppType> appTypes ) {
        elasticsearchApi.triggerAppTypeIndex( appTypes );
    }

    public void triggerAllOrganizationsIndex( List<Organization> allOrganizations ) {
        elasticsearchApi.triggerOrganizationIndex( allOrganizations );
    }

    public void triggerOrganizationIndex( Organization organization ) {
        elasticsearchApi.triggerOrganizationIndex( Lists.newArrayList( organization ) );
    }

}
