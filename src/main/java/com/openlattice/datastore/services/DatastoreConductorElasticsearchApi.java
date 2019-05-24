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
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.conductor.rpc.*;
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.EntityDataKeySearchResult;
import com.openlattice.search.requests.SearchConstraints;
import com.openlattice.search.requests.SearchResult;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DatastoreConductorElasticsearchApi implements ConductorElasticsearchApi {

    private static final Logger logger = LoggerFactory.getLogger( DatastoreConductorElasticsearchApi.class );

    private final DurableExecutorService executor;

    public DatastoreConductorElasticsearchApi( HazelcastInstance hazelcast ) {
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    @Override
    public boolean saveEntitySetToElasticsearch(
            EntityType entityType,
            EntitySet entitySet,
            List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.submitEntitySetToElasticsearch(
                            entityType,
                            entitySet,
                            propertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity set to elasticsearch" );
            return false;
        }
    }

    @Override public Set<UUID> getEntityTypesWithIndices() {
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
    public boolean deleteEntitySet( UUID entitySetId, UUID entityTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntitySet( entitySetId, entityTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity set from elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean clearEntitySetData( UUID entitySetId, UUID entityTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.clearEntitySetData( entitySetId, entityTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to clear entity set data from elasticsearch" );
            return false;
        }
    }

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
    public boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> updatedPropertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updatePropertyTypesInEntitySet( entitySetId,
                            updatedPropertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update property types in entity set in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean addPropertyTypesToEntityType( EntityType entityType, List<PropertyType> newPropertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.addPropertyTypesToEntityType( entityType, newPropertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to add property types to entity type in elasticsearch" );
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
            logger.error( "Unable to to perform organization keyword search.", e );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeEntitySetCollectionSearch(
            String searchTerm,
            Set<AclKey> authorizedEntitySetCollectionIds,
            int start,
            int maxHits ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.executeEntitySetCollectionSearch( searchTerm,
                            authorizedEntitySetCollectionIds,
                            start,
                            maxHits ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to to perform EntitySetCollection keyword search.", e );
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
    public boolean triggerSecurableObjectIndex(
            SecurableObjectType securableObjectType,
            Iterable<?> securableObjects ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerSecurableObjectIndex( securableObjectType, securableObjects ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger app type re-index" );
            return false;
        }
    }

    @Override
    public boolean createEntityData(
            UUID entityTypeId,
            EntityDataKey edk,
            Map<UUID, Set<Object>> propertyValues ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new EntityDataLambdas( entityTypeId, edk, propertyValues ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity data to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createBulkEntityData(
            UUID entityTypeId,
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entitiesById ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new BulkEntityDataLambdas( entityTypeId, entitySetId, entitiesById ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity data to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createBulkLinkedData(
            UUID entityTypeId,
            UUID entitySetId,
            Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByLinkingId ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new BulkLinkedDataLambdas( entityTypeId, entitySetId, entitiesByLinkingId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save linked entity data to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteEntityData( EntityDataKey edk, UUID entityTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntityData( edk, entityTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity data from elasticsearch" );
            return false;
        }
    }

    @Override public boolean deleteEntityDataBulk( UUID entitySetId, UUID entityTypeId, Set<UUID> entityKeyIds ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntityDataBulk( entitySetId, entityTypeId, entityKeyIds ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity data from elasticsearch" );
            return false;
        }
    }

    @Override public EntityDataKeySearchResult executeSearch(
            SearchConstraints searchConstraints,
            Map<UUID, UUID> entityTypesByEntitySetId,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet,
            Map<UUID, DelegatedUUIDSet> linkingEntitySets ) {
        try {
            EntityDataKeySearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new SearchWithConstraintsLambda( searchConstraints,
                            entityTypesByEntitySetId,
                            authorizedPropertyTypesByEntitySet,
                            linkingEntitySets ) ) )
                    .get();
            return queryResults;

        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute entity set data search with constraints" );
            return new EntityDataKeySearchResult( 0, Sets.newHashSet() );
        }
    }

    @Override
    public Map<UUID, Set<UUID>> executeBlockingSearch(
            UUID entityTypeId, Map<UUID, DelegatedStringSet> fieldSearches, int size, boolean explain ) {
        throw new NotImplementedException( "BLAME MTR. This is for linking only." );
    }

    @Override
    public boolean saveEntityTypeToElasticsearch( EntityType entityType, List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveEntityTypeToElasticsearch( entityType, propertyTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity type to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean saveAssociationTypeToElasticsearch(
            AssociationType associationType,
            List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveAssociationTypeToElasticsearch( associationType, propertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save association type to elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeSecurableObjectSearch(
            SecurableObjectType securableObjectType, String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeSecurableObjectSearch(
                            securableObjectType,
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute securable object search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeSecurableObjectFQNSearch(
            SecurableObjectType securableObjectType, String namespace, String name, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeSecurableObjectFQNSearch(
                            securableObjectType,
                            namespace,
                            name,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute securable object FQN search" );
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

    @Override public boolean saveSecurableObjectToElasticsearch(
            SecurableObjectType securableObjectType, Object securableObject ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas
                            .saveSecurableObjectToElasticsearch( securableObjectType, securableObject ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save securable object to elasticsearch" );
            return false;
        }
    }

    @Override public boolean deleteSecurableObjectFromElasticsearch(
            SecurableObjectType securableObjectType, UUID objectId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteSecurableObject( securableObjectType, objectId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete securable object from elasticsearch" );
            return false;
        }
    }

}
