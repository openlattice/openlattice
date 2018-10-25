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

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.search.SearchService;
import com.openlattice.edm.EntitySet;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( SearchApi.CONTROLLER )
public class SearchController implements SearchApi, AuthorizingComponent {

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

        Map<UUID, Map<UUID, PropertyType>> authorizedProperties = authorizationsHelper
                .getAuthorizedPropertiesOnEntitySets( Sets.newHashSet( searchConstraints.getEntitySetIds() ),
                        EnumSet.of( Permission.READ ) );
        return searchService.executeSearch( searchConstraints, authorizedProperties );
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
        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edm.getEntitySet( entitySetId );
            if( es.isLinking() && !es.getLinkedEntitySets().isEmpty() ) {
                Map<UUID, Map<UUID, PropertyType>> authorizedProperties = authorizationsHelper
                        .getAuthorizedPropertiesOnEntitySets( es.getLinkedEntitySets(), EnumSet.of( Permission.READ ) );
                if ( !authorizedProperties.isEmpty() && !es.getLinkedEntitySets().isEmpty() ) {
                    return searchService.executeLinkingSearch( SearchConstraints
                            .simpleSearchConstraints(
                                    es.getLinkedEntitySets().toArray(new UUID[es.getLinkedEntitySets().size()]),
                                    searchTerm.getStart(),
                                    searchTerm.getMaxHits(),
                                    searchTerm.getSearchTerm(),
                                    searchTerm.getFuzzy() ),
                            authorizedProperties );
                }
            } else {
                Map<UUID, Map<UUID, PropertyType>> authorizedProperties = authorizationsHelper
                        .getAuthorizedPropertiesOnEntitySets( Set.of( entitySetId ),
                                EnumSet.of( Permission.READ ) );
                if ( !authorizedProperties.values().isEmpty() ) {
                    return searchService.executeSearch( SearchConstraints
                            .simpleSearchConstraints( new UUID[] { entitySetId },
                                    searchTerm.getStart(),
                                    searchTerm.getMaxHits(),
                                    searchTerm.getSearchTerm(),
                                    searchTerm.getFuzzy() ),
                            authorizedProperties);
                }
            }
        }
        return new DataSearchResult( 0, Lists.newArrayList() );
    }

    @RequestMapping(
            path = { ADVANCED + ENTITY_SET_ID_PATH },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public DataSearchResult executeAdvancedEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody AdvancedSearch search ) {
        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {

            EntitySet es = edm.getEntitySet( entitySetId );
            if( es.isLinking() ) {
                Preconditions.checkArgument(
                        !es.getLinkedEntitySets().isEmpty(),
                        "Linked entity sets does not consist of any entity sets." );
                Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authorizationsHelper
                        .getAuthorizedPropertiesOnEntitySets( es.getLinkedEntitySets(), EnumSet.of(Permission.READ) );

                Set<UUID> propertyTypeIds = authorizedPropertyTypes.values().stream().map( pt -> pt.keySet() )
                        .reduce( (s1, s2) -> Sets.intersection(s1, s2) ).get();

                // only those are authorized, where propertytype is authorized for every entity set
                List<SearchDetails> authorizedSearches = search.getSearches().stream()
                        .filter( searchDetails -> propertyTypeIds.contains( searchDetails.getPropertyType() ) )
                        .collect( Collectors.toList() );

                return searchService.executeLinkingSearch(
                        SearchConstraints.advancedSearchConstraints(
                                es.getLinkedEntitySets().toArray(new UUID[es.getLinkedEntitySets().size()]),
                                search.getStart(),
                                search.getMaxHits(),
                                authorizedSearches ),
                        authorizedPropertyTypes );
            } else {
                Map<UUID, Map<UUID, PropertyType>> authorizedProperties = authorizationsHelper
                        .getAuthorizedPropertiesOnEntitySets( Set.of( entitySetId ),
                                EnumSet.of( Permission.READ ) );

                Set<UUID> propertyTypeIds = authorizedProperties.get( entitySetId ).keySet();

                List<SearchDetails> authorizedSearches = search.getSearches().stream()
                        .filter( searchDetails -> propertyTypeIds.contains( searchDetails.getPropertyType() ) )
                        .collect( Collectors.toList() );

                return searchService.executeSearch( SearchConstraints.advancedSearchConstraints( new UUID[] { entitySetId },
                        search.getStart(),
                        search.getMaxHits(),
                        authorizedSearches ),
                        authorizedProperties );
            }
        }
        return new DataSearchResult( 0, Lists.newArrayList() );
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
            @PathVariable( ENTITY_ID ) UUID entityId ) {
        // TODO linked
        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ), Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edm.getEntitySet( entitySetId );
            if( es.isLinking() ) {
                Preconditions.checkArgument(
                        !es.getLinkedEntitySets().isEmpty(),
                        "Linked entity sets does not consist of any entity sets." );
                Set<UUID> authorizedEntitySets = es.getLinkedEntitySets().stream()
                        .filter( linkedEntitySetId ->
                                authorizations.checkIfHasPermissions( new AclKey( linkedEntitySetId ),
                                        Principals.getCurrentPrincipals(),
                                        EnumSet.of( Permission.READ ) ) )
                        .collect( Collectors.toSet() );
                if( authorizedEntitySets.size() == 0 ) {
                    logger.warn( "Read authorization failed for all the linked entity sets." );
                    return Lists.newArrayList();
                }
                return searchService.executeLinkingEntityNeighborSearch( authorizedEntitySets, ImmutableSet.of( entityId ) )
                        .get( entityId );
            } else {
                return searchService.executeEntityNeighborSearch( ImmutableSet.of( entitySetId ), ImmutableSet.of( entityId ) )
                        .get( entityId );
            }
        }
        return Lists.newArrayList();
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NEIGHBORS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public Map<UUID, List<NeighborEntityDetails>> executeEntityNeighborSearchBulk(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Set<UUID> entityIds ) {
        Map<UUID, List<NeighborEntityDetails>> result = Maps.newHashMap();
        if ( authorizations.checkIfHasPermissions( new AclKey( entitySetId ), Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edm.getEntitySet( entitySetId );
            if( es.isLinking() ) {
                Preconditions.checkArgument(
                        !es.getLinkedEntitySets().isEmpty(),
                        "Linking entity sets does not consist of any entity sets." );
                Set<UUID> authorizedEntitySets = es.getLinkedEntitySets().stream()
                        .filter( linkedEntitySetId ->
                                authorizations.checkIfHasPermissions( new AclKey( linkedEntitySetId ),
                                        Principals.getCurrentPrincipals(),
                                        EnumSet.of( Permission.READ ) ) )
                        .collect( Collectors.toSet() );
                if( authorizedEntitySets.size() == 0 ) {
                    logger.warn( "Read authorization failed for all the linked entity sets." );
                    return result;
                }

                result = searchService.executeLinkingEntityNeighborSearch( authorizedEntitySets, entityIds );
            } else {
                result = searchService.executeEntityNeighborSearch( ImmutableSet.of( entitySetId ), entityIds );
            }
        }
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
    public Void trigerEntitySetDataIndex( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
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
}
