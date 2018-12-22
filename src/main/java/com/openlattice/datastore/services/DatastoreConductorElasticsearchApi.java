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

package com.openlattice.datastore.services;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.openlattice.apps.App;
import com.openlattice.apps.AppType;
import com.openlattice.authorization.AclKey;
import com.openlattice.conductor.rpc.*;
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.*;

import java.util.Optional;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DatastoreConductorElasticsearchApi implements ConductorElasticsearchApi {

    private static final Logger logger = LoggerFactory.getLogger( DatastoreConductorElasticsearchApi.class );

    private final DurableExecutorService executor;

    public DatastoreConductorElasticsearchApi( HazelcastInstance hazelcast ) {
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    @Override
    public boolean saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.submitEntitySetToElasticsearch( entitySet, propertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity set to elasticsearch" );
            return false;
        }
    }

    @Override public Set<UUID> getEntitySetWithIndices() {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.getEntitySetsWithIndices() ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to retrieve list of indexes." );
            return ImmutableSet.of();
        }
    }

    @Override
    public boolean createSecurableObjectIndex( UUID entitySetId, List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.createSecurableObjectIndex(
                            entitySetId,
                            propertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity set to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteEntitySet( UUID entitySetId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntitySet( entitySetId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity set from elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean clearEntitySetData( UUID entitySetId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.clearEntitySetData( entitySetId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to clear entity set data from elasticsearch" );
            return false;
        }    }

    @Override
    public SearchResult executeEntitySetMetadataSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits ) {
        try {
            SearchResult searchResult = executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.executeEntitySetMetadataQuery( optionalSearchTerm,
                            optionalEntityType,
                            optionalPropertyTypes,
                            authorizedAclKeys,
                            start,
                            maxHits ) ) )
                    .get();
            return searchResult;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to to perofrm keyword search.", e );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public boolean updateEntitySetMetadata( EntitySet entitySet ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updateEntitySetMetadata( entitySet ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update entity set metadata in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updatePropertyTypesInEntitySet( entitySetId,
                            newPropertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update property types in entity set in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createOrganization( Organization organization ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.createOrganization( organization ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to create organization in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteOrganization( UUID organizationId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteOrganization( organizationId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete organization from elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits ) {
        try {
            SearchResult searchResult = executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.executeOrganizationKeywordSearch( searchTerm,
                            authorizedOrganizationIds,
                            start,
                            maxHits ) ) )
                    .get();
            return searchResult;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to to perform keyword search.", e );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updateOrganization( id,
                            optionalTitle,
                            optionalDescription ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update organization in elasticsearch" );
            return false;
        }
    }

    @Override public boolean triggerOrganizationIndex( List<Organization> organizations ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerOrganizationIndex( organizations ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger app type re-index" );
            return false;
        }
    }

    @Override
    public boolean createEntityData(
            EntityDataKey edk,
            Map<UUID, Set<Object>> propertyValues ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new EntityDataLambdas( edk, propertyValues ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity data to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createBulkEntityData( UUID entitySetId, Map<UUID, Map<UUID, Set<Object>>> entitiesById ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new BulkEntityDataLambdas( entitySetId, entitiesById ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity data to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteEntityData( EntityDataKey edk ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntityData( edk ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity data from elasticsearch" );
            return false;
        }
    }

    @Override public EntityDataKeySearchResult executeSearch(
            SearchConstraints searchConstraints, Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet ) {
        try {
            EntityDataKeySearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new SearchWithConstraintsLambda( searchConstraints, authorizedPropertyTypesByEntitySet ) ) ).get();
            return queryResults;

        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute entity set data search with constraints" );
            return new EntityDataKeySearchResult( 0, Sets.newHashSet() );
        }
    }

    @Override public Map<UUID, Set<UUID>> searchEntitySets(
            Iterable<UUID> entitySetIds, Map<UUID, DelegatedStringSet> fieldSearches, int size, boolean explain ) {
        throw new NotImplementedException( "BLAME MTR. This is for linking only." );
    }

    @Override
    public List<UUID> executeEntitySetDataSearchAcrossIndices(
            Iterable<UUID> entitySetIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        try {
            List<UUID> queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new SearchEntitySetDataAcrossIndicesLambda( entitySetIds, fieldSearches, size, explain ) ) )
                    .get();
            return queryResults;

        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Failed to execute search for entity set data search across indices: " + fieldSearches );
            return Lists.newArrayList();
        }
    }

    @Override
    public boolean saveEntityTypeToElasticsearch( EntityType entityType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveEntityTypeToElasticsearch( entityType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity type to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean saveAssociationTypeToElasticsearch( AssociationType associationType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveAssociationTypeToElasticsearch( associationType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save association type to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean savePropertyTypeToElasticsearch( PropertyType propertyType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.savePropertyTypeToElasticsearch( propertyType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save property type to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteEntityType( UUID entityTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntityType( entityTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity type from elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteAssociationType( UUID associationTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteAssociationType( associationTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete association type from elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deletePropertyType( UUID propertyTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deletePropertyType( propertyTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete property type from elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeEntityTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute entity type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeAssociationTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute association type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executePropertyTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute property type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeFQNEntityTypeSearch(
                            namespace,
                            name,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute property type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeFQNPropertyTypeSearch(
                            namespace,
                            name,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute property type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public boolean clearAllData() {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.clearAllData() ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete all data" );
            return false;
        }
    }

    @Override
    public double getModelScore( double[][] features ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.getModelScore( features ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to get model score" );
            return Double.MAX_VALUE;
        }
    }

    @Override
    public boolean triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerPropertyTypeIndex( propertyTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger property type re-index" );
            return false;
        }
    }

    @Override
    public boolean triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerEntityTypeIndex( entityTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger entity type re-index" );
            return false;
        }
    }

    @Override
    public boolean triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerAssociationTypeIndex( associationTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger association type re-index" );
            return false;
        }
    }

    @Override
    public boolean triggerEntitySetIndex(
            Map<EntitySet, Set<UUID>> entitySets,
            Map<UUID, PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerEntitySetIndex( entitySets, propertyTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger entity set re-index" );
            return false;
        }
    }

    @Override public boolean triggerAppIndex( List<App> apps ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerAppIndex( apps ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger app re-index" );
            return false;
        }
    }

    @Override public boolean triggerAppTypeIndex( List<AppType> appTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerAppTypeIndex( appTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger app type re-index" );
            return false;
        }
    }

    @Override public boolean saveAppToElasticsearch( App app ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveAppToElasticsearch( app ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save app to elasticsearch" );
            return false;
        }
    }

    @Override public boolean deleteApp( UUID appId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteApp( appId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete app from elasticsearch" );
            return false;
        }
    }

    @Override public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeAppSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute app search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override public boolean saveAppTypeToElasticsearch( AppType appType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveAppTypeToElasticsearch( appType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save app type to elasticsearch" );
            return false;
        }
    }

    @Override public boolean deleteAppType( UUID appTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteAppType( appTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete app type from elasticsearch" );
            return false;
        }
    }

    @Override public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeAppTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute app type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

}
