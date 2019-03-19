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

package com.openlattice.datastore.search.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import com.openlattice.auditing.AuditEventType;
import com.openlattice.auditing.AuditRecordEntitySetsManager;
import com.openlattice.auditing.AuditableEvent;
import com.openlattice.auditing.AuditingComponent;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.data.requests.NeighborEntityIds;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.search.SearchApi;
import com.openlattice.search.SearchService;
import com.openlattice.search.requests.*;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.openlattice.postgres.DataTables.ID_FQN;

@RestController
@RequestMapping( SearchApi.CONTROLLER )
public class SearchController implements SearchApi, AuthorizingComponent, AuditingComponent {

    @Inject
    private SearchService searchService;

    @Inject
    private EdmService edm;

    @Inject
    private AppService appService;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private EdmAuthorizationHelper authorizationsHelper;

    @Inject
    private HazelcastOrganizationService organizationService;

    @Inject
    private SecurePrincipalsManager spm;

    @Inject
    private ObjectMapper mapper;

    @Inject
    private AuditRecordEntitySetsManager auditRecordEntitySetsManager;

    @Inject
    private DataGraphManager dgm;

    @RequestMapping(
            path = { "/", "" },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeEntitySetKeywordQuery(
            @RequestBody Search search ) {
        if ( !search.getOptionalKeyword().isPresent() && !search.getOptionalEntityType().isPresent()
                && !search.getOptionalPropertyTypes().isPresent() ) {
            throw new IllegalArgumentException(
                    "Your search cannot be empty--you must include at least one of of the three params: keyword ('kw'), entity type id ('eid'), or property type ids ('pid')" );
        }
        return searchService
                .executeEntitySetKeywordSearchQuery( search.getOptionalKeyword(),
                        search.getOptionalEntityType(),
                        search.getOptionalPropertyTypes(),
                        search.getStart(),
                        search.getMaxHits() );
    }

    @RequestMapping(
            path = { POPULAR },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public Iterable<EntitySet> getPopularEntitySet() {
        return edm.getEntitySets();

    }

    @RequestMapping(
            path = { ENTITY_SETS + START_PATH + NUM_RESULTS_PATH },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult getEntitySets(
            @PathVariable( START ) int start,
            @PathVariable( NUM_RESULTS ) int maxHits ) {
        return searchService
                .executeEntitySetKeywordSearchQuery( Optional.of( "*" ),
                        Optional.empty(),
                        Optional.empty(),
                        start,
                        Math.min( maxHits, SearchApi.MAX_SEARCH_RESULTS ) );
    }

    @RequestMapping(
            path = { "/", "" },
            method = RequestMethod.PATCH,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public DataSearchResult searchEntitySetData( @RequestBody SearchConstraints searchConstraints ) {
        Map<UUID, EntitySet> entitySets = edm.getEntitySetsAsMap( Set.of( searchConstraints.getEntitySetIds() ) );

        // We have to search "normal" and linking entity sets differently, because property authorization by entity sets
        Map<UUID, Map<UUID, PropertyType>> authorizedProperties = authorizationsHelper
                .getAuthorizedPropertiesOnEntitySets(
                        entitySets.values().stream()
                                .filter( entitySet -> !entitySet.isLinking() )
                                .map( EntitySet::getId )
                                .collect( Collectors.toSet() ),
                        EnumSet.of( Permission.READ ) );
        Map<UUID, Map<UUID, PropertyType>> linkedAuthorizedProperties = authorizationsHelper
                .getAuthorizedPropertiesOnEntitySets(
                        entitySets.values().stream()
                                .filter( EntitySet::isLinking )
                                .flatMap( entitySet -> entitySet.getLinkedEntitySets().stream() )
                                .collect( Collectors.toSet() ),
                        EnumSet.of( Permission.READ ) );

        DataSearchResult results = searchService.executeSearch( searchConstraints, authorizedProperties, false );
        DataSearchResult linkedResults = searchService.executeSearch(
                searchConstraints,
                linkedAuthorizedProperties,
                true );

        DataSearchResult mergedResults = mergeDataSearchResults( results, linkedResults );

        List<AuditableEvent> searchEvents = Lists.newArrayList();
        for ( int i = 0; i < searchConstraints.getEntitySetIds().length; i++ ) {
            searchEvents.add( new AuditableEvent(
                    getCurrentUserId(),
                    new AclKey( searchConstraints.getEntitySetIds()[ i ] ),
                    AuditEventType.SEARCH_ENTITY_SET_DATA,
                    "Entity set data searched through SearchApi.searchEntitySetData",
                    Optional.of( getEntityKeyIdsFromSearchResult( mergedResults ) ),
                    ImmutableMap.of( "query", searchConstraints ),
                    OffsetDateTime.now(),
                    Optional.empty()
            ) );
        }

        recordEvents( searchEvents );

        return mergedResults;
    }

    private DataSearchResult mergeDataSearchResults(
            DataSearchResult results1,
            DataSearchResult results2 ) {
        List<SetMultimap<FullQualifiedName, Object>> mergedHits = Streams
                .concat( results1.getHits().stream(), results2.getHits().stream() )
                .collect( Collectors.toList() );

        return new DataSearchResult( results1.getNumHits() + results2.getNumHits(), mergedHits );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    @RequestMapping(
            path = { ORGANIZATIONS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeOrganizationSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeOrganizationKeywordSearch( searchTerm );
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public DataSearchResult executeEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody SearchTerm searchTerm ) {
        DataSearchResult dataSearchResult = new DataSearchResult( 0, Lists.newArrayList() );

        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edm.getEntitySet( entitySetId );
            final Set<UUID> entitySets = ( es.isLinking() ) ? es.getLinkedEntitySets() : Set.of( entitySetId );
            final Set<UUID> authorizedEntitySets = entitySets.stream().filter( esId ->
                    authorizations.checkIfHasPermissions(
                            new AclKey( esId ),
                            Principals.getCurrentPrincipals(),
                            EnumSet.of( Permission.READ ) ) )
                    .collect( Collectors.toSet() );
            Map<UUID, Map<UUID, PropertyType>> authorizedProperties = authorizationsHelper
                    .getAuthorizedPropertiesOnEntitySets( authorizedEntitySets, EnumSet.of( Permission.READ ) );

            if ( authorizedEntitySets.size() == 0 ) {
                logger.warn( "Read authorization failed for all the entity sets." );
            } else {

                dataSearchResult = searchService.executeSearch( SearchConstraints
                                .simpleSearchConstraints(
                                        new UUID[] { entitySetId },
                                        searchTerm.getStart(),
                                        searchTerm.getMaxHits(),
                                        searchTerm.getSearchTerm(),
                                        searchTerm.getFuzzy() ),
                        authorizedProperties,
                        es.isLinking() );
            }
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.SEARCH_ENTITY_SET_DATA,
                "Entity set data search through SearchApi.executeEntitySetDataQuery",
                Optional.of( getEntityKeyIdsFromSearchResult( dataSearchResult ) ),
                ImmutableMap.of( "query", searchTerm ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return dataSearchResult;
    }

    @RequestMapping(
            path = { ADVANCED + ENTITY_SET_ID_PATH },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public DataSearchResult executeAdvancedEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody AdvancedSearch search ) {
        DataSearchResult dataSearchResult = new DataSearchResult( 0, Lists.newArrayList() );

        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {

            EntitySet es = edm.getEntitySet( entitySetId );
            final Set<UUID> entitySets = ( es.isLinking() ) ? es.getLinkedEntitySets() : Set.of( entitySetId );
            final Set<UUID> authorizedEntitySets = entitySets.stream().filter( esId ->
                    authorizations.checkIfHasPermissions(
                            new AclKey( esId ),
                            Principals.getCurrentPrincipals(),
                            EnumSet.of( Permission.READ ) ) )
                    .collect( Collectors.toSet() );
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authorizationsHelper
                    .getAuthorizedPropertiesOnEntitySets( authorizedEntitySets, EnumSet.of( Permission.READ ) );

            if ( authorizedEntitySets.size() == 0 ) {
                logger.warn( "Read authorization failed for all the entity sets." );
            } else {
                dataSearchResult = searchService.executeSearch(
                        SearchConstraints.advancedSearchConstraints(
                                new UUID[] { entitySetId },
                                search.getStart(),
                                search.getMaxHits(),
                                search.getSearches() ),
                        authorizedPropertyTypes,
                        es.isLinking() );
            }
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.SEARCH_ENTITY_SET_DATA,
                "Advanced entity set data search through SearchApi.executeAdvancedEntitySetDataQuery",
                Optional.of( getEntityKeyIdsFromSearchResult( dataSearchResult ) ),
                ImmutableMap.of( "query", search ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return dataSearchResult;
    }

    @RequestMapping(
            path = { ENTITY_TYPES },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeEntityTypeSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeEntityTypeSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ASSOCIATION_TYPES },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeAssociationTypeSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeAssociationTypeSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { PROPERTY_TYPES },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executePropertyTypeSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executePropertyTypeSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { APP },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeAppSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeAppSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { APP_TYPES },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeAppTypeSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeAppTypeSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ENTITY_TYPES + FQN },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeFQNEntityTypeSearch( @RequestBody FQNSearchTerm searchTerm ) {
        return searchService.executeFQNEntityTypeSearch( searchTerm.getNamespace(),
                searchTerm.getName(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { PROPERTY_TYPES + FQN },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeFQNPropertyTypeSearch( @RequestBody FQNSearchTerm searchTerm ) {
        return searchService.executeFQNPropertyTypeSearch( searchTerm.getNamespace(),
                searchTerm.getName(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + ENTITY_ID_PATH },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public List<NeighborEntityDetails> executeEntityNeighborSearch(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_ID ) UUID entityKeyId ) {
        List<NeighborEntityDetails> neighbors = Lists.newArrayList();

        Set<Principal> principals = Principals.getCurrentPrincipals();

        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ), principals,
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edm.getEntitySet( entitySetId );
            if ( es.isLinking() ) {
                Preconditions.checkArgument(
                        !es.getLinkedEntitySets().isEmpty(),
                        "Linked entity sets does not consist of any entity sets." );
                Set<UUID> authorizedEntitySets = es.getLinkedEntitySets().stream()
                        .filter( linkedEntitySetId ->
                                authorizations.checkIfHasPermissions( new AclKey( linkedEntitySetId ),
                                        Principals.getCurrentPrincipals(),
                                        EnumSet.of( Permission.READ ) ) )
                        .collect( Collectors.toSet() );
                if ( authorizedEntitySets.size() == 0 ) {
                    logger.warn( "Read authorization failed for all the linked entity sets." );
                } else {
                    neighbors = searchService
                            .executeLinkingEntityNeighborSearch( authorizedEntitySets,
                                    new EntityNeighborsFilter( ImmutableSet.of( entityKeyId ) ), principals )
                            .getOrDefault( entityKeyId, ImmutableList.of() );
                }
            } else {
                neighbors = searchService
                        .executeEntityNeighborSearch( ImmutableSet.of( entitySetId ),
                                new EntityNeighborsFilter( ImmutableSet.of( entityKeyId ) ), principals )
                        .getOrDefault( entityKeyId, ImmutableList.of() );
            }
        }

        UUID userId = getCurrentUserId();

        SetMultimap<UUID, UUID> neighborsByEntitySet = HashMultimap.create();
        neighbors.forEach( neighborEntityDetails -> {
            neighborsByEntitySet.put( neighborEntityDetails.getAssociationEntitySet().getId(),
                    getEntityKeyId( neighborEntityDetails.getAssociationDetails() ) );
            if ( neighborEntityDetails.getNeighborEntitySet().isPresent() ) {
                neighborsByEntitySet.put( neighborEntityDetails.getNeighborEntitySet().get().getId(),
                        getEntityKeyId( neighborEntityDetails.getNeighborDetails().get() ) );
            }
        } );

        List<AuditableEvent> events = new ArrayList<>( neighborsByEntitySet.keySet().size() + 1 );
        events.add( new AuditableEvent(
                userId,
                new AclKey( entitySetId ),
                AuditEventType.LOAD_ENTITY_NEIGHBORS,
                "Load entity neighbors through SearchApi.executeEntityNeighborSearch",
                Optional.of( ImmutableSet.of( entityKeyId ) ),
                ImmutableMap.of(),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        for ( UUID neighborEntitySetId : neighborsByEntitySet.keySet() ) {
            events.add( new AuditableEvent(
                    userId,
                    new AclKey( neighborEntitySetId ),
                    AuditEventType.READ_ENTITIES,
                    "Read entities as neighbors through SearchApi.executeEntityNeighborSearch",
                    Optional.of( neighborsByEntitySet.get( neighborEntitySetId ) ),
                    ImmutableMap.of( "aclKeySearched", new AclKey( entitySetId, entityKeyId ) ),
                    OffsetDateTime.now(),
                    Optional.empty()
            ) );
        }

        recordEvents( events );

        return neighbors;
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NEIGHBORS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public Map<UUID, List<NeighborEntityDetails>> executeEntityNeighborSearchBulk(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Set<UUID> entityKeyIds ) {
        return executeFilteredEntityNeighborSearch( entitySetId, new EntityNeighborsFilter( entityKeyIds ) );
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NEIGHBORS + ADVANCED },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public Map<UUID, List<NeighborEntityDetails>> executeFilteredEntityNeighborSearch(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody EntityNeighborsFilter filter ) {
        Set<Principal> principals = Principals.getCurrentPrincipals();

        Map<UUID, List<NeighborEntityDetails>> result = Maps.newHashMap();
        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ), principals,
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edm.getEntitySet( entitySetId );
            if ( es.isLinking() ) {
                Preconditions.checkArgument(
                        !es.getLinkedEntitySets().isEmpty(),
                        "Linking entity sets does not consist of any entity sets." );
                Set<UUID> authorizedEntitySets = es.getLinkedEntitySets().stream()
                        .filter( linkedEntitySetId ->
                                authorizations.checkIfHasPermissions( new AclKey( linkedEntitySetId ),
                                        Principals.getCurrentPrincipals(),
                                        EnumSet.of( Permission.READ ) ) )
                        .collect( Collectors.toSet() );
                if ( authorizedEntitySets.size() == 0 ) {
                    logger.warn( "Read authorization failed for all the linked entity sets." );
                } else {
                    result = searchService
                            .executeLinkingEntityNeighborSearch( authorizedEntitySets, filter, principals );
                }

            } else {
                result = searchService
                        .executeEntityNeighborSearch( ImmutableSet.of( entitySetId ), filter, principals );
            }
        }


        /* audit */

        ListMultimap<UUID, UUID> neighborsByEntitySet = ArrayListMultimap.create();

        result.values().forEach( neighborList -> {
            neighborList.forEach( neighborEntityDetails -> {
                neighborsByEntitySet.put( neighborEntityDetails.getAssociationEntitySet().getId(),
                        getEntityKeyId( neighborEntityDetails.getAssociationDetails() ) );
                if ( neighborEntityDetails.getNeighborEntitySet().isPresent() ) {
                    neighborsByEntitySet.put( neighborEntityDetails.getNeighborEntitySet().get().getId(),
                            getEntityKeyId( neighborEntityDetails.getNeighborDetails().get() ) );
                }
            } );
        } );

        List<AuditableEvent> events = new ArrayList<>( neighborsByEntitySet.keySet().size() + 1 );
        UUID userId = getCurrentUserId();

        int segments = filter.getEntityKeyIds().size() / AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
        if ( filter.getEntityKeyIds().size() % AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT != 0 ) {
            segments++;
        }

        List<UUID> entityKeyIdsAsList = Lists.newArrayList( filter.getEntityKeyIds() );

        for ( int i = 0; i < segments; i++ ) {

            int fromIndex = i * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
            int toIndex = i == segments - 1 ?
                    entityKeyIdsAsList.size() :
                    ( i + 1 ) * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;

            Set<UUID> segmentOfIds = Sets.newHashSet( entityKeyIdsAsList.subList( fromIndex, toIndex ));

            events.add( new AuditableEvent(
                    userId,
                    new AclKey( entitySetId ),
                    AuditEventType.LOAD_ENTITY_NEIGHBORS,
                    "Load neighbors of entities with filter through SearchApi.executeFilteredEntityNeighborSearch",
                    Optional.of( segmentOfIds ),
                    ImmutableMap.of( "filters", new EntityNeighborsFilter( segmentOfIds, filter.getSrcEntitySetIds(), filter.getDstEntitySetIds(), filter.getAssociationEntitySetIds() ) ),
                    OffsetDateTime.now(),
                    Optional.empty()
            ) );
        }

        for ( UUID neighborEntitySetId : neighborsByEntitySet.keySet() ) {
            List<UUID> neighbors = neighborsByEntitySet.get( neighborEntitySetId );

            int neighborSegments = neighbors.size() / AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
            if ( neighbors.size() % AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT != 0 ) {
                neighborSegments++;
            }

            for ( int i = 0; i < neighborSegments; i++ ) {

                int fromIndex = i * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
                int toIndex = i == neighborSegments - 1 ?
                        neighbors.size() :
                        ( i + 1 ) * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;

                events.add( new AuditableEvent(
                        userId,
                        new AclKey( neighborEntitySetId ),
                        AuditEventType.READ_ENTITIES,
                        "Read entities as filtered neighbors through SearchApi.executeFilteredEntityNeighborSearch",
                        Optional.of( Sets.newHashSet( neighbors.subList( fromIndex, toIndex ) ) ),
                        ImmutableMap.of( "entitySetId", entitySetId ),
                        OffsetDateTime.now(),
                        Optional.empty()
                ) );
            }
        }

        recordEvents( events );

        return result;
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NEIGHBORS + ADVANCED + IDS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public Map<UUID, Map<UUID, SetMultimap<UUID, NeighborEntityIds>>> executeFilteredEntityNeighborIdsSearch(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody EntityNeighborsFilter filter ) {
        Set<Principal> principals = Principals.getCurrentPrincipals();

        Map<UUID, Map<UUID, SetMultimap<UUID, NeighborEntityIds>>> result = Maps.newHashMap();
        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ), principals,
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edm.getEntitySet( entitySetId );
            if ( es.isLinking() ) {
                Preconditions.checkArgument(
                        !es.getLinkedEntitySets().isEmpty(),
                        "Linking entity sets does not consist of any entity sets." );
                Set<UUID> authorizedEntitySets = es.getLinkedEntitySets().stream()
                        .filter( linkedEntitySetId ->
                                authorizations.checkIfHasPermissions( new AclKey( linkedEntitySetId ),
                                        Principals.getCurrentPrincipals(),
                                        EnumSet.of( Permission.READ ) ) )
                        .collect( Collectors.toSet() );
                if ( authorizedEntitySets.size() == 0 ) {
                    logger.warn( "Read authorization failed for all the linked entity sets." );
                } else {
                    result = searchService
                            .executeLinkingEntityNeighborIdsSearch( authorizedEntitySets, filter, principals );
                }

            } else {
                result = searchService
                        .executeEntityNeighborIdsSearch( ImmutableSet.of( entitySetId ), filter, principals );
            }
        }


        /* audit */

        ListMultimap<UUID, UUID> neighborsByEntitySet = ArrayListMultimap.create();

        result.values().forEach( associationMap -> {
            associationMap.entrySet().forEach( associationEntry -> {
                associationEntry.getValue().entries().forEach( neighborEntry -> {
                    neighborsByEntitySet.put( associationEntry.getKey(), neighborEntry.getValue().getAssociationEntityKeyId() );
                    neighborsByEntitySet.put( neighborEntry.getKey(), neighborEntry.getValue().getNeighborEntityKeyId() );
                } );
            } );
        } );

        List<AuditableEvent> events = new ArrayList<>( neighborsByEntitySet.keySet().size() + 1 );
        UUID userId = getCurrentUserId();

        int segments = filter.getEntityKeyIds().size() / AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
        if ( filter.getEntityKeyIds().size() % AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT != 0 ) {
            segments++;
        }

        List<UUID> entityKeyIdsAsList = Lists.newArrayList( filter.getEntityKeyIds() );

        for ( int i = 0; i < segments; i++ ) {

            int fromIndex = i * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
            int toIndex = i == segments - 1 ?
                    entityKeyIdsAsList.size() :
                    ( i + 1 ) * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;

            Set<UUID> segmentOfIds = Sets.newHashSet( entityKeyIdsAsList.subList( fromIndex, toIndex ));

            events.add( new AuditableEvent(
                    userId,
                    new AclKey( entitySetId ),
                    AuditEventType.LOAD_ENTITY_NEIGHBORS,
                    "Load neighbors of entities with filter through SearchApi.executeFilteredEntityNeighborIdsSearch",
                    Optional.of( segmentOfIds ),
                    ImmutableMap.of( "filters", new EntityNeighborsFilter( segmentOfIds, filter.getSrcEntitySetIds(), filter.getDstEntitySetIds(), filter.getAssociationEntitySetIds() ) ),
                    OffsetDateTime.now(),
                    Optional.empty()
            ) );
        }

        for ( UUID neighborEntitySetId : neighborsByEntitySet.keySet() ) {
            List<UUID> neighbors = neighborsByEntitySet.get( neighborEntitySetId );

            int neighborSegments = neighbors.size() / AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
            if ( neighbors.size() % AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT != 0 ) {
                neighborSegments++;
            }

            for ( int i = 0; i < neighborSegments; i++ ) {

                int fromIndex = i * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
                int toIndex = i == neighborSegments - 1 ?
                        neighbors.size() :
                        ( i + 1 ) * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;

                events.add( new AuditableEvent(
                        userId,
                        new AclKey( neighborEntitySetId ),
                        AuditEventType.READ_ENTITIES,
                        "Read entities as filtered neighbors through SearchApi.executeFilteredEntityNeighborIdsSearch",
                        Optional.of( Sets.newHashSet( neighbors.subList( fromIndex, toIndex ) ) ),
                        ImmutableMap.of( "entitySetId", entitySetId ),
                        OffsetDateTime.now(),
                        Optional.empty()
                ) );
            }
        }

        recordEvents( events );

        return result;
    }

    @RequestMapping(
            path = { EDM + INDEX },
            method = RequestMethod.GET )
    @Override
    public Void triggerEdmIndex() {
        ensureAdminAccess();
        searchService.triggerPropertyTypeIndex( Lists.newArrayList( edm.getPropertyTypes() ) );
        searchService.triggerEntityTypeIndex( Lists.newArrayList( edm.getEntityTypes() ) );
        searchService.triggerAssociationTypeIndex( Lists.newArrayList( edm.getAssociationTypes() ) );
        searchService.triggerEntitySetIndex();
        searchService.triggerAppIndex( Lists.newArrayList( appService.getApps() ) );
        searchService.triggerAppTypeIndex( Lists.newArrayList( appService.getAppTypes() ) );
        return null;
    }

    @RequestMapping(
            path = { ENTITY_SETS + INDEX + ENTITY_SET_ID_PATH },
            method = RequestMethod.GET )
    @Override
    public Void triggerEntitySetDataIndex( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureAdminAccess();
        searchService.triggerEntitySetDataIndex( entitySetId );
        return null;
    }

    @RequestMapping(
            path = { ENTITY_SETS + INDEX },
            method = RequestMethod.GET )
    @Override
    public Void triggerAllEntitySetDataIndex() {
        ensureAdminAccess();
        searchService.triggerAllEntitySetDataIndex();
        return null;
    }

    @RequestMapping(
            path = { ORGANIZATIONS + INDEX },
            method = RequestMethod.GET )
    @Override
    public Void triggerAllOrganizationsIndex() {
        ensureAdminAccess();
        List<Organization> allOrganizations = Lists.newArrayList(
                organizationService.getOrganizations(
                        getAccessibleObjects( SecurableObjectType.Organization,
                                EnumSet.of( Permission.READ ) ) // TODO: other access check??
                                .parallel()
                                .filter( Predicates.notNull()::apply )
                                .map( AuthorizationUtils::getLastAclKeySafely ) ) );
        searchService.triggerAllOrganizationsIndex( allOrganizations );
        return null;
    }

    @RequestMapping(
            path = { ORGANIZATIONS + INDEX + ORGANIZATION_ID_PATH },
            method = RequestMethod.GET )
    @Override
    public Void triggerOrganizationIndex( @PathVariable( ORGANIZATION_ID ) UUID organizationId ) {
        ensureAdminAccess();
        searchService.triggerOrganizationIndex( organizationService.getOrganization( organizationId ) );
        return null;
    }

    private UUID getCurrentUserId() {
        return spm.getPrincipal( Principals.getCurrentUser().getId() ).getId();
    }

    private static Set<UUID> getEntityKeyIdsFromSearchResult( DataSearchResult searchResult ) {
        return searchResult.getHits().stream().map( SearchController::getEntityKeyId ).collect( Collectors.toSet() );
    }

    private static UUID getEntityKeyId( SetMultimap<FullQualifiedName, Object> entity ) {
        return UUID.fromString( entity.get( ID_FQN ).iterator().next().toString() );
    }

    @NotNull
    @Override
    public AuditRecordEntitySetsManager getAuditRecordEntitySetsManager() {
        return auditRecordEntitySetsManager;
    }

    @NotNull
    @Override
    public DataGraphManager getDataGraphService() {
        return dgm;
    }
}
