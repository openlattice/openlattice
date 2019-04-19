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

package com.openlattice.kindling.search;

import com.dataloom.mappers.ObjectMappers;
import com.dataloom.streams.StreamUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.*;
import com.openlattice.apps.App;
import com.openlattice.apps.AppType;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.conductor.rpc.SearchConfiguration;
import com.openlattice.data.EntityDataKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.Analyzer;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class ConductorElasticsearchImpl implements ConductorElasticsearchApi {
    // @formatter:off
    private static final int MAX_CONCURRENT_SEARCHES = 3;

    private static final ObjectMapper mapper = ObjectMappers.newJsonMapper();
    private static final Logger       logger = LoggerFactory
            .getLogger( ConductorElasticsearchImpl.class );

    private static final String[] DEFAULT_INDICES = new String[] {
            ENTITY_SET_DATA_MODEL,
            ORGANIZATIONS,
            ENTITY_TYPE_INDEX,
            ASSOCIATION_TYPE_INDEX,
            PROPERTY_TYPE_INDEX,
            APP_INDEX,
            APP_TYPE_INDEX
    };

    static {
        mapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
    }

    private       Client                              client;
    private       ElasticsearchTransportClientFactory factory;
    private       boolean                             connected = true;
    private       String                              server;
    private       String                              cluster;
    private       int                                 port;
    // @formatter:on

    public ConductorElasticsearchImpl( SearchConfiguration config ) {
        this( config, Optional.empty() );
    }

    public ConductorElasticsearchImpl(
            SearchConfiguration config,
            Optional<Client> someClient ) {
        init( config );
        client = someClient.orElseGet( factory::getClient );
        initializeIndices();
    }

    private void init( SearchConfiguration config ) {
        server = config.getElasticsearchUrl();
        cluster = config.getElasticsearchCluster();
        port = config.getElasticsearchPort();
        factory = new ElasticsearchTransportClientFactory( server, port, cluster );
    }

    /* INDEX CREATION */

    public void initializeIndices() {
        for ( String indexName : DEFAULT_INDICES ) {
            createIndex( indexName );
        }
    }

    private boolean createIndex( String indexName ) {
        switch ( indexName ) {
            case ENTITY_SET_DATA_MODEL:
                return initializeEntitySetDataModelIndex();
            case ORGANIZATIONS:
                return initializeOrganizationIndex();
            case ENTITY_TYPE_INDEX:
                return initializeEntityTypeIndex();
            case ASSOCIATION_TYPE_INDEX:
                return initializeAssociationTypeIndex();
            case PROPERTY_TYPE_INDEX:
                return initializePropertyTypeIndex();
            case APP_INDEX:
                return initializeAppIndex();
            case APP_TYPE_INDEX:
                return initializeAppTypeIndex();
            default: {
                logger.error( "Unable to initialize index {}", indexName );
                return false;
            }
        }
    }

    @Override
    public Set<UUID> getEntityTypesWithIndices() {
        return Stream.of( client.admin().indices().prepareGetIndex().setFeatures().get().getIndices() )
                .filter( s -> s.startsWith( DATA_INDEX_PREFIX ) )
                .map( s -> UUID.fromString( s.substring( DATA_INDEX_PREFIX.length() ) ) )
                .collect( toSet() );
    }

    // @formatter:off
    private XContentBuilder getMetaphoneSettings( int numShards ) throws IOException {
    	XContentBuilder settings = XContentFactory.jsonBuilder()
    	        .startObject()
        	        .startObject( ANALYSIS )
                        .startObject( FILTER )
                            .startObject( METAPHONE_FILTER )
                                .field( TYPE, PHONETIC )
                                .field( ENCODER, METAPHONE )
                                .field( REPLACE, false )
                            .endObject()
                            .startObject( SHINGLE_FILTER )
                                .field( TYPE, SHINGLE )
                                .field( OUTPUT_UNIGRAMS, true )
                                .field( TOKEN_SEPARATOR, "" )
                            .endObject()
                        .endObject()
            	        .startObject( ANALYZER )
                	        .startObject( METAPHONE_ANALYZER )
                	            .field( TOKENIZER, LOWERCASE )
                	            .field( FILTER, Lists.newArrayList( STANDARD, LOWERCASE, SHINGLE_FILTER, METAPHONE_FILTER ) )
                	        .endObject()
                	    .endObject()
        	        .endObject()
        	        .field( NUM_SHARDS, numShards )
        	        .field( NUM_REPLICAS, 2 )
    	        .endObject();
    	return settings;
    }
    // @formatter:on

    private boolean initializeEntitySetDataModelIndex() {
        if ( !verifyElasticsearchConnection() ) { return false; }

        boolean exists = client.admin().indices()
                .prepareExists( ENTITY_SET_DATA_MODEL ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        // constant Map<String, String> type fields
        Map<String, String> objectField = Maps.newHashMap();
        Map<String, String> nestedField = Maps.newHashMap();
        //Map<String, String> keywordField = Maps.newHashMap();
        objectField.put( TYPE, OBJECT );
        nestedField.put( TYPE, NESTED );
        //keywordField.put( TYPE, KEYWORD );

        // entity_set type mapping
        Map<String, Object> properties = Maps.newHashMap();
        Map<String, Object> entitySetData = Maps.newHashMap();
        Map<String, Object> mapping = Maps.newHashMap();
        properties.put( PROPERTY_TYPES, nestedField );
        properties.put( ENTITY_SET, objectField );
        properties.put( ENTITY_SET + "." + NAME, ImmutableMap.of( TYPE, TEXT, ANALYZER, METAPHONE_ANALYZER ) );
        properties.put( ENTITY_SET + "." + TITLE, ImmutableMap.of( TYPE, TEXT, ANALYZER, METAPHONE_ANALYZER ) );
        properties.put( ENTITY_SET + "." + DESCRIPTION, ImmutableMap.of( TYPE, TEXT, ANALYZER, METAPHONE_ANALYZER ) );
        entitySetData.put( MAPPING_PROPERTIES, properties );
        mapping.put( ENTITY_SET_TYPE, entitySetData );

        try {
            client.admin().indices().prepareCreate( ENTITY_SET_DATA_MODEL )
                    .setSettings( getMetaphoneSettings( 5 ) )
                    .addMapping( ENTITY_SET_TYPE, mapping )
                    .execute().actionGet();
            return true;
        } catch ( IOException e ) {
            logger.error( "Unable to initialize entity set data model index", e );
            return false;
        }
    }

    private boolean initializeOrganizationIndex() {
        if ( !verifyElasticsearchConnection() ) { return false; }

        boolean exists = client.admin().indices()
                .prepareExists( ORGANIZATIONS ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        // constant Map<String, String> type fields
        Map<String, String> objectField = Maps.newHashMap();
        Map<String, String> keywordField = Maps.newHashMap();
        objectField.put( TYPE, OBJECT );
        keywordField.put( TYPE, KEYWORD );

        // entity_set type mapping
        Map<String, Object> properties = Maps.newHashMap();
        Map<String, Object> organizationData = Maps.newHashMap();
        Map<String, Object> organizationMapping = Maps.newHashMap();
        properties.put( ORGANIZATION, objectField );
        organizationData.put( MAPPING_PROPERTIES, properties );
        organizationMapping.put( ORGANIZATION_TYPE, organizationData );

        client.admin().indices().prepareCreate( ORGANIZATIONS )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 5 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ORGANIZATION_TYPE, organizationMapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializeEntityTypeIndex() {
        if ( !verifyElasticsearchConnection() ) { return false; }

        boolean exists = client.admin().indices()
                .prepareExists( ENTITY_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( ENTITY_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( ENTITY_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 5 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ENTITY_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializeAssociationTypeIndex() {
        if ( !verifyElasticsearchConnection() ) { return false; }

        boolean exists = client.admin().indices()
                .prepareExists( ASSOCIATION_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( ASSOCIATION_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( ASSOCIATION_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 5 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ASSOCIATION_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializePropertyTypeIndex() {
        if ( !verifyElasticsearchConnection() ) { return false; }

        boolean exists = client.admin().indices()
                .prepareExists( PROPERTY_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( PROPERTY_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( PROPERTY_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 5 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( PROPERTY_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializeAppIndex() {
        if ( !verifyElasticsearchConnection() ) { return false; }

        boolean exists = client.admin().indices()
                .prepareExists( APP_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( APP, Maps.newHashMap() );
        client.admin().indices().prepareCreate( APP_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 5 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( APP, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializeAppTypeIndex() {
        if ( !verifyElasticsearchConnection() ) { return false; }

        boolean exists = client.admin().indices()
                .prepareExists( APP_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( APP_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( APP_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 5 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( APP_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private Map<String, String> getFieldMapping( PropertyType propertyType ) {
        Map<String, String> fieldMapping = Maps.newHashMap();
        switch ( propertyType.getDatatype() ) {
            case Boolean: {
                fieldMapping.put( TYPE, BOOLEAN );
                break;
            }
            case SByte:
            case Byte: {
                fieldMapping.put( TYPE, BYTE );
                break;
            }
            case Decimal: {
                fieldMapping.put( TYPE, FLOAT );
                break;
            }
            case Double:
            case Single: {
                fieldMapping.put( TYPE, DOUBLE );
                break;
            }
            case Int16: {
                fieldMapping.put( TYPE, SHORT );
                break;
            }
            case Int32: {
                fieldMapping.put( TYPE, INTEGER );
                break;
            }
            case Int64: {
                fieldMapping.put( TYPE, LONG );
                break;
            }
            case String: {
                String analyzer = ( propertyType.getAnalyzer().equals( Analyzer.METAPHONE ) ) ? METAPHONE_ANALYZER
                        : STANDARD;
                fieldMapping.put( TYPE, TEXT );
                fieldMapping.put( ANALYZER, analyzer );
                break;
            }
            case Date:
            case DateTimeOffset: {
                fieldMapping.put( TYPE, DATE );
                break;
            }
            case GeographyPoint: {
                fieldMapping.put( TYPE, GEO_POINT );
                break;
            }
            case Guid:
            default: {
                fieldMapping.put( INDEX, "false" );
                fieldMapping.put( TYPE, KEYWORD );
            }
        }

        if ( propertyType.getAnalyzer().equals( Analyzer.NOT_ANALYZED ) ) {
            fieldMapping.put( ANALYZER, KEYWORD );
        }
        return fieldMapping;
    }

    private String getIndexName( UUID entityTypeId ) {
        return DATA_INDEX_PREFIX + entityTypeId.toString();
    }

    private String getTypeName( UUID entityTypeId ) {
        return DATA_TYPE_PREFIX + entityTypeId.toString();
    }

    /*** ENTITY DATA INDEX CREATE / DELETE / UPDATE ***/

    public boolean createEntityTypeDataIndex( EntityType entityType, List<PropertyType> propertyTypes ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        UUID entityTypeId = entityType.getId();

        String indexName = getIndexName( entityTypeId );
        String typeName = getTypeName( entityTypeId );

        boolean exists = client.admin().indices()
                .prepareExists( indexName ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        final Map<String, Object> entityTypeMapping = prepareEntityTypeDataMappings( typeName, propertyTypes );

        try {
            client.admin().indices().prepareCreate( indexName )
                    .setSettings( getMetaphoneSettings( entityType.getShards() ) )
                    .addMapping( typeName, entityTypeMapping )
                    .execute().actionGet();
        } catch ( IOException e ) {
            logger.debug( "unable to create entity type data index for {}", entityTypeId );
        }
        return true;
    }

    private boolean addMappingToEntityTypeDataIndex(
            EntityType entityType,
            List<PropertyType> propertyTypes ) {

        String indexName = getIndexName( entityType.getId() );
        String typeName = getTypeName( entityType.getId() );

        final Map<String, Object> entityTypeDataMapping = prepareEntityTypeDataMappings( typeName, propertyTypes );

        PutMappingRequest request = new PutMappingRequest( indexName );
        request.type( typeName );
        request.source( entityTypeDataMapping );
        try {
            client.admin().indices().putMapping( request ).actionGet();
        } catch ( IllegalStateException e ) {
            logger.debug( "unable to add mapping to entity type data index for {}", entityType.getId() );
        }
        return true;
    }

    private Map<String, Object> prepareEntityTypeDataMappings(
            String typeName,
            List<PropertyType> propertyTypes ) {
        Map<String, Object> keywordMapping = ImmutableMap.of( TYPE, KEYWORD );
        // securable_object_row type mapping
        Map<String, Object> entityTypeDataMapping = Maps.newHashMap();
        Map<String, Object> fieldMappings = Maps.newHashMap();
        Map<String, Object> properties = Maps.newHashMap();
        Map<String, Object> entityMapping = Maps.newHashMap();
        Map<String, Object> entityPropertiesMapping = Maps.newHashMap();

        entityPropertiesMapping.put( ENTITY_SET_ID_KEY.toString(), keywordMapping );
        entityPropertiesMapping.put( LAST_WRITE.toString(), keywordMapping );

        for ( PropertyType propertyType : propertyTypes ) {

            if ( !propertyType.getDatatype().equals( EdmPrimitiveTypeKind.Binary ) ) {
                entityPropertiesMapping.put( propertyType.getId().toString(), getFieldMapping( propertyType ) );
            }
        }

        entityMapping.put( MAPPING_PROPERTIES, entityPropertiesMapping );
        entityMapping.put( TYPE, NESTED );

        properties.put( ENTITY, entityMapping );
        properties.put( ENTITY_SET_ID_FIELD, keywordMapping );

        fieldMappings.put( MAPPING_PROPERTIES, properties );

        entityTypeDataMapping.put( typeName, fieldMappings );

        return entityTypeDataMapping;
    }

    @Override
    public boolean saveEntitySetToElasticsearch(
            EntityType entityType,
            EntitySet entitySet,
            List<PropertyType> propertyTypes ) {
        if ( !verifyElasticsearchConnection() ) { return false; }
        Map<String, Object> entitySetDataModel = Maps.newHashMap();
        entitySetDataModel.put( ENTITY_SET, entitySet );
        entitySetDataModel.put( PROPERTY_TYPES, propertyTypes );

        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( entitySetDataModel );
            client.prepareIndex( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySet.getId().toString() )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();

            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving entity set to elasticsearch" );
        }
        return false;
    }

    /**
     * Add new mappings to existing index.
     * Updating the entity set model is handled in {@link #updatePropertyTypesInEntitySet(UUID, List)}
     *
     * @param entityType       the entity type to which the new properties are added
     * @param newPropertyTypes the ids of the new properties
     */
    @Override
    public boolean addPropertyTypesToEntityType( EntityType entityType, List<PropertyType> newPropertyTypes ) {
        saveObjectToElasticsearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityType, entityType.getId().toString() );
        return addMappingToEntityTypeDataIndex( entityType, newPropertyTypes );
    }

    @Override
    public boolean deleteEntitySet( UUID entitySetId, UUID entityTypeId ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        client.prepareDelete( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetId.toString() ).execute().actionGet();

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder( client )
                .filter( QueryBuilders.termQuery( ENTITY_SET_ID_FIELD, entitySetId.toString() ) )
                .source( getIndexName( entityTypeId ) )
                .get();

        logger.info( "Deleted {} documents from index {} for entity set {}",
                response.getDeleted(),
                entityTypeId,
                entitySetId );

        return true;
    }

    private UUID getEntitySetIdFromHit( SearchHit hit ) {
        return UUID.fromString( hit.getMatchedQueries()[ 0 ] );
    }

    /*** ENTITY DATA CREATE/DELETE ***/

    private byte[] formatLinkedEntity( Map<UUID, Map<UUID, Set<Object>>> entityValuesByEntitySet ) {

        List<Map<Object, Object>> documents = entityValuesByEntitySet.entrySet().stream().flatMap( esEntry -> {
            UUID entitySetId = esEntry.getKey();
            Map<UUID, Set<Object>> entity = esEntry.getValue();

            List<Map<Object, Object>> values = Lists.newArrayList();
            entity.entrySet().forEach( entry -> values.add( ImmutableMap.of( entry.getKey(), entry.getValue() ) ) );
            values.add( ImmutableMap.of( ENTITY_SET_ID_KEY, entitySetId ) );

            return values.stream();
        } ).collect( Collectors.toList() );

        try {
            return mapper.writeValueAsBytes( ImmutableMap.of( ENTITY, documents ) );

        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating entity data" );
            return null;
        }
    }

    private byte[] formatEntity( UUID entitySetId, Map<UUID, Set<Object>> entity ) {

        List<Map<Object, Object>> values = Lists.newArrayList();
        entity.entrySet().forEach( entry -> values.add( ImmutableMap.of( entry.getKey(), entry.getValue() ) ) );
        values.add( ImmutableMap.of( ENTITY_SET_ID_KEY, entitySetId ) );

        try {
            return mapper.writeValueAsBytes( ImmutableMap.of( ENTITY, values, ENTITY_SET_ID_FIELD, entitySetId ) );

        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating entity data" );
            return null;
        }
    }

    @Override
    public boolean createEntityData( UUID entityTypeId, EntityDataKey edk, Map<UUID, Set<Object>> propertyValues ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        UUID entitySetId = edk.getEntitySetId();
        UUID entityKeyId = edk.getEntityKeyId();

        byte[] data = formatEntity( entitySetId, propertyValues );

        if ( data != null ) {
            client.prepareIndex( getIndexName( entityTypeId ), getTypeName( entityTypeId ), entityKeyId.toString() )
                    .setSource( data, XContentType.JSON )
                    .execute().actionGet();
        }

        return data != null;
    }

    @Override
    public boolean createBulkEntityData(
            UUID entityTypeId,
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entitiesById ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        if ( !entitiesById.isEmpty() ) {
            String indexName = getIndexName( entityTypeId );
            String indexType = getTypeName( entityTypeId );

            BulkRequestBuilder requestBuilder = client.prepareBulk();
            for ( Map.Entry<UUID, Map<UUID, Set<Object>>> entitiesByIdEntry : entitiesById.entrySet() ) {

                final UUID entityKeyId = entitiesByIdEntry.getKey();

                byte[] data = formatEntity( entitySetId, entitiesByIdEntry.getValue() );

                if ( data != null ) {
                    requestBuilder.add(
                            client.prepareIndex( indexName, indexType, entityKeyId.toString() )
                                    .setSource( data, XContentType.JSON ) );
                }
            }

            requestBuilder.execute().actionGet();

        }
        return true;
    }

    @Override
    public boolean createBulkLinkedData(
            UUID entityTypeId,
            UUID linkingEntitySetId,
            Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByLinkingId ) { // linking_id/entity_set_id/property_id
        if ( !verifyElasticsearchConnection() ) { return false; }

        if ( !entitiesByLinkingId.isEmpty() ) {
            String indexName = getIndexName( entityTypeId );
            String indexType = getTypeName( entityTypeId );

            BulkRequestBuilder requestBuilder = client.prepareBulk();
            for ( Map.Entry<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByLinkingIdEntry : entitiesByLinkingId
                    .entrySet() ) {
                final UUID linkingId = entitiesByLinkingIdEntry.getKey();

                final byte[] data = formatLinkedEntity( entitiesByLinkingIdEntry.getValue() );

                requestBuilder.add(
                        client.prepareIndex( indexName, indexType, linkingId.toString() )
                                .setSource( data, XContentType.JSON ) );
            }
            requestBuilder.execute().actionGet();

        }
        return true;
    }

    @Override
    public boolean deleteEntityData( EntityDataKey edk, UUID entityTypeId ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        UUID entityKeyId = edk.getEntityKeyId(); // either entityKeyId or linkingId

        client.prepareDelete( getIndexName( entityTypeId ), getTypeName( entityTypeId ), entityKeyId.toString() )
                .execute()
                .actionGet();
        return true;
    }

    @Override
    public boolean deleteEntityDataBulk( UUID entitySetId, UUID entityTypeId, Set<UUID> entityKeyIds ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        String index = getIndexName( entityTypeId );
        String type = getTypeName( entityTypeId );

        BulkRequestBuilder request = client.prepareBulk();
        entityKeyIds.forEach( entityKeyId -> request.add( new DeleteRequest( index, type, entityKeyId.toString() ) ) );

        request.execute().actionGet();

        return true;
    }

    @Override
    public boolean clearEntitySetData( UUID entitySetId, UUID entityTypeId ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE )
                .filter( QueryBuilders.termQuery( ENTITY_SET_ID_FIELD, entitySetId.toString() ) )
                .source( getIndexName( entityTypeId ) )
                .execute()
                .actionGet();

        // TODO linked entity set data

        return true;
    }

    /*** ENTITY DATA SEARCH HELPERS ***/

    private EntityDataKeySearchResult getEntityDataKeySearchResult( SearchResponse response ) {
        Set<EntityDataKey> entityDataKeys = Sets.newHashSet();
        for ( SearchHit hit : response.getHits() ) {
            entityDataKeys.add( new EntityDataKey( getEntitySetIdFromHit( hit ),
                    UUID.fromString( hit.getId() ) ) );
        }
        return new EntityDataKeySearchResult( response.getHits().getTotalHits(), entityDataKeys );
    }

    private EntityDataKeySearchResult getEntityDataKeySearchResult( MultiSearchResponse response ) {
        Set<EntityDataKey> entityDataKeys = Sets.newHashSet();
        var totalHits = 0;
        for ( MultiSearchResponse.Item item : response.getResponses() ) {
            for ( SearchHit hit : item.getResponse().getHits() ) {
                entityDataKeys.add( new EntityDataKey( getEntitySetIdFromHit( hit ),
                        UUID.fromString( hit.getId() ) ) );
            }
            totalHits += item.getResponse().getHits().totalHits;
        }
        return new EntityDataKeySearchResult( totalHits, entityDataKeys );
    }

    /**
     * Creates for each authorized property type a map with key of that property type id and puts 1 weights as value.
     */
    private static Map<UUID, Map<String, Float>> getFieldsMap(
            UUID entitySetId,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet,
            Optional<Set<UUID>> linkedEntitySets ) {
        Map<UUID, Map<String, Float>> fieldsMap = Maps.newHashMap();
        authorizedPropertyTypesByEntitySet.get( entitySetId ).forEach( propertyTypeId -> {

            String fieldName = getFieldName( propertyTypeId );

            if ( linkedEntitySets.isPresent() ) {

                boolean isAuthorized = true;

                for ( UUID normalEntitySetId : linkedEntitySets.get() ) {
                    if ( !authorizedPropertyTypesByEntitySet
                            .getOrDefault( normalEntitySetId, DelegatedUUIDSet.wrap( ImmutableSet.of() ) )
                            .contains( propertyTypeId ) ) {
                        isAuthorized = false;
                    }
                }

                if ( isAuthorized ) {
                    fieldsMap.put( propertyTypeId, Map.of( fieldName, 1F ) );
                }

            } else {
                fieldsMap.put( propertyTypeId, Map.of( fieldName, 1F ) );
            }
        } );
        return fieldsMap;
    }

    private static String getFieldName( UUID propertyTypeId ) {
        return ENTITY + "." + propertyTypeId.toString();
    }

    private QueryBuilder getAdvancedSearchQuery(
            Constraint constraints,
            Map<UUID, Map<String, Float>> authorizedFieldsMap ) {

        BoolQueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch( 1 );
        constraints.getSearches().get().forEach( search -> {
            if ( authorizedFieldsMap.keySet().contains( search.getPropertyType() ) ) {

                QueryStringQueryBuilder queryString = QueryBuilders
                        .queryStringQuery( search.getSearchTerm() )
                        .fields( authorizedFieldsMap.get( search.getPropertyType() ) ).lenient( true );

                if ( search.getExactMatch() ) {
                    query.must( queryString );
                    query.minimumShouldMatch( 0 );
                } else {
                    query.should( queryString );
                }
            }
        } );

        return query;
    }

    private QueryBuilder getSimpleSearchQuery(
            Constraint constraints,
            Map<UUID, Map<String, Float>> authorizedFieldsMap ) {

        String searchTerm = constraints.getSearchTerm().get();
        boolean fuzzy = constraints.getFuzzy().get();

        String formattedSearchTerm = fuzzy ? getFormattedFuzzyString( searchTerm ) : searchTerm;

        return QueryBuilders.queryStringQuery( formattedSearchTerm )
                .fields( authorizedFieldsMap.values().stream()
                        .flatMap( fieldsMap -> fieldsMap.entrySet().stream() )
                        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) ) )
                .lenient( true );
    }

    private QueryBuilder getGeoDistanceSearchQuery(
            Constraint constraints,
            Map<UUID, Map<String, Float>> authorizedFieldsMap ) {

        UUID propertyTypeId = constraints.getPropertyTypeId().get();
        if ( authorizedFieldsMap.getOrDefault( propertyTypeId, ImmutableMap.of() ).size() == 0 ) {
            return null;
        }

        double latitude = constraints.getLatitude().get();
        double longitude = constraints.getLongitude().get();
        double radius = constraints.getRadius().get();

        BoolQueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch( 1 );

        authorizedFieldsMap.get( propertyTypeId ).keySet().forEach( fieldName ->
                query.should( QueryBuilders
                        .geoDistanceQuery( fieldName )
                        .point( latitude, longitude )
                        .distance( radius, DistanceUnit.fromString( constraints.getDistanceUnit().get().name() ) ) )
        );

        return query;
    }

    private QueryBuilder getGeoPolygonSearchQuery(
            Constraint constraints,
            Map<UUID, Map<String, Float>> authorizedFieldsMap ) {

        UUID propertyTypeId = constraints.getPropertyTypeId().get();
        if ( authorizedFieldsMap.getOrDefault( propertyTypeId, ImmutableMap.of() ).size() == 0 ) {
            return null;
        }

        BoolQueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch( 1 );

        for ( List<List<Double>> zone : constraints.getZones().get() ) {
            List<GeoPoint> polygon = zone.stream().map( pair -> new GeoPoint( pair.get( 1 ), pair.get( 0 ) ) )
                    .collect( Collectors.toList() );

            authorizedFieldsMap.get( propertyTypeId ).keySet()
                    .forEach( fieldName -> query.should( QueryBuilders.geoPolygonQuery( fieldName, polygon ) ) );
        }

        return query;
    }

    private QueryBuilder getWriteDateTimeFilterQuery( UUID[] entitySetIds, Constraint constraint ) {
        BoolQueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch( 1 );

        for ( int i = 0; i < entitySetIds.length; i++ ) {
            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery( getFieldName( LAST_WRITE ) );

            if ( constraint.getStartDate().isPresent() ) {
                rangeQuery.gt( constraint.getStartDate().get().toString() );
            }

            if ( constraint.getEndDate().isPresent() ) {
                rangeQuery.lte( constraint.getEndDate().get().toString() );
            }

            query.should( rangeQuery );
        }

        return query;
    }

    private QueryBuilder getQueryForSearch(
            UUID entitySetId,
            SearchConstraints searchConstraints,
            Map<UUID, Map<String, Float>> authorizedFieldsMap ) {
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        for ( ConstraintGroup constraintGroup : searchConstraints.getConstraintGroups() ) {
            BoolQueryBuilder subQuery = QueryBuilders.boolQuery()
                    .minimumShouldMatch( constraintGroup.getMinimumMatches() );

            for ( Constraint constraint : constraintGroup.getConstraints() ) {

                QueryBuilder constraintQuery;

                switch ( constraint.getSearchType() ) {
                    case advanced:
                        subQuery.should( getAdvancedSearchQuery( constraint, authorizedFieldsMap ) );
                        break;

                    case geoDistance:
                        subQuery.should( getGeoDistanceSearchQuery( constraint, authorizedFieldsMap ) );
                        break;

                    case geoPolygon:
                        subQuery.should( getGeoPolygonSearchQuery( constraint, authorizedFieldsMap ) );
                        break;

                    case simple:
                        subQuery.should( getSimpleSearchQuery( constraint, authorizedFieldsMap ) );
                        break;

                    case writeDateTimeFilter:
                        subQuery.should( getWriteDateTimeFilterQuery( searchConstraints.getEntitySetIds(),
                                constraint ) );
                        break;

                }
            }

            query.must( QueryBuilders.nestedQuery( ENTITY, subQuery, ScoreMode.Total ) );
        }

        query.must( QueryBuilders.nestedQuery( ENTITY,
                QueryBuilders.termQuery( getFieldName( ENTITY_SET_ID_KEY ), entitySetId.toString() ),
                ScoreMode.None ) );
        return query;
    }

    /*** ENTITY DATA SEARCH ***/

    @Override
    public EntityDataKeySearchResult executeSearch(
            SearchConstraints searchConstraints,
            Map<UUID, UUID> entityTypesByEntitySetId,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet,
            Map<UUID, DelegatedUUIDSet> linkingEntitySets ) {
        if ( !verifyElasticsearchConnection() ) {
            return new EntityDataKeySearchResult( 0, Sets.newHashSet() );
        }

        MultiSearchRequest requests = new MultiSearchRequest().maxConcurrentSearchRequests( MAX_CONCURRENT_SEARCHES );

        for ( int i = 0; i < searchConstraints.getEntitySetIds().length; i++ ) {
            UUID entitySetId = searchConstraints.getEntitySetIds()[ i ];

            Map<UUID, Map<String, Float>> authorizedFieldsMap =
                    getFieldsMap( entitySetId,
                            authorizedPropertyTypesByEntitySet,
                            Optional.ofNullable( linkingEntitySets.get( entitySetId ) ) );

            BoolQueryBuilder query = new BoolQueryBuilder().queryName( entitySetId.toString() )
                    .must( getQueryForSearch( entitySetId, searchConstraints, authorizedFieldsMap ) );

            if ( linkingEntitySets.containsKey( entitySetId ) ) {
                query.mustNot( QueryBuilders
                        .existsQuery( ENTITY_SET_ID_FIELD ) ); // this field will not exist for linked entity documents
            } else {
                query.must( QueryBuilders
                        .termQuery( ENTITY_SET_ID_FIELD, entitySetId.toString() ) ); // match entity set id
            }

            SearchRequestBuilder request = client
                    .prepareSearch( getIndexName( entityTypesByEntitySetId.get( entitySetId ) ) )
                    .setQuery( query )
                    .setFrom( searchConstraints.getStart() )
                    .setSize( searchConstraints.getMaxHits() )
                    .setFetchSource( false );
            requests.add( request );
        }

        MultiSearchResponse response = client.multiSearch( requests ).actionGet();
        return getEntityDataKeySearchResult( response );
    }

    @Override
    public Map<UUID, Set<UUID>> executeBlockingSearch(
            UUID entityTypeId,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        if ( !verifyElasticsearchConnection() ) { return null; }

        BoolQueryBuilder valuesQuery = new BoolQueryBuilder();

        fieldSearches.entrySet().stream().forEach( entry -> {

            BoolQueryBuilder fieldQuery = new BoolQueryBuilder();
            entry.getValue().stream().forEach( searchTerm -> fieldQuery.should(
                    QueryBuilders.matchQuery( getFieldName( entry.getKey() ), searchTerm ).fuzziness( Fuzziness.AUTO )
                            .lenient( true ) ) );
            fieldQuery.minimumShouldMatch( 1 );
            valuesQuery.should( QueryBuilders.nestedQuery( ENTITY, fieldQuery, ScoreMode.Avg ) );
        } );

        valuesQuery.minimumShouldMatch( 1 );

        BoolQueryBuilder query = QueryBuilders.boolQuery().must( valuesQuery )
                .must( QueryBuilders.existsQuery( ENTITY_SET_ID_FIELD ) );

        return StreamUtil.stream( client.prepareSearch( getIndexName( entityTypeId ) )
                .setQuery( query )
                .setFrom( 0 )
                .setSize( size )
                .setExplain( explain )
                .setFetchSource( ENTITY_SET_ID_FIELD, null )
                .execute()
                .actionGet().getHits() )
                .map( hit -> Pair
                        .of( UUID.fromString( hit.getSourceAsMap().get( ENTITY_SET_ID_FIELD ).toString() ),
                                UUID.fromString( hit.getId() ) ) )
                .collect( Collectors.groupingBy( Pair::getKey, Collectors.mapping( Pair::getValue, toSet() ) ) );
    }

    /*** EDM OBJECT CRUD TRIGGERING INDEX UPDATES ***/

    @Override
    public boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        Map<String, Object> updatedFields = Maps.newHashMap();
        if ( optionalTitle.isPresent() ) {
            updatedFields.put( TITLE, optionalTitle.get() );
        }
        if ( optionalDescription.isPresent() ) {
            updatedFields.put( DESCRIPTION, optionalDescription.get() );
        }
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( updatedFields );
            UpdateRequest updateRequest = new UpdateRequest( ORGANIZATIONS, ORGANIZATION_TYPE, id.toString() )
                    .doc( s, XContentType.JSON );
            client.update( updateRequest ).actionGet();
            return true;
        } catch ( IOException e ) {
            logger.debug( "error updating organization in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean saveEntityTypeToElasticsearch( EntityType entityType, List<PropertyType> propertyTypes ) {
        saveObjectToElasticsearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityType, entityType.getId().toString() );
        return createEntityTypeDataIndex( entityType, propertyTypes );
    }

    @Override
    public boolean saveAssociationTypeToElasticsearch(
            AssociationType associationType,
            List<PropertyType> propertyTypes ) {
        EntityType entityType = associationType.getAssociationEntityType();
        if ( entityType == null ) {
            logger.debug( "An association type must have an entity type present in order to save to elasticsearch" );
            return false;
        }

        saveObjectToElasticsearch( ASSOCIATION_TYPE_INDEX,
                ASSOCIATION_TYPE,
                associationType,
                entityType.getId().toString() );

        return createEntityTypeDataIndex( entityType, propertyTypes );
    }

    @Override
    public boolean savePropertyTypeToElasticsearch( PropertyType propertyType ) {
        return saveObjectToElasticsearch( PROPERTY_TYPE_INDEX,
                PROPERTY_TYPE,
                propertyType,
                propertyType.getId().toString() );
    }

    @Override
    public boolean saveAppToElasticsearch( App app ) {
        return saveObjectToElasticsearch( APP_INDEX, APP, app, app.getId().toString() );
    }

    @Override
    public boolean saveAppTypeToElasticsearch( AppType appType ) {
        return saveObjectToElasticsearch( APP_TYPE_INDEX, APP_TYPE, appType, appType.getId().toString() );
    }

    @Override
    public boolean deleteEntityType( UUID entityTypeId ) {
        client.admin().indices()
                .delete( new DeleteIndexRequest( getIndexName( entityTypeId ) ) );

        return deleteObjectById( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityTypeId.toString() );
    }

    @Override
    public boolean deleteAssociationType( UUID associationTypeId ) {
        client.admin().indices()
                .delete( new DeleteIndexRequest( getIndexName( associationTypeId ) ) );

        return deleteObjectById( ASSOCIATION_TYPE_INDEX, ASSOCIATION_TYPE, associationTypeId.toString() );
    }

    @Override
    public boolean deletePropertyType( UUID propertyTypeId ) {
        return deleteObjectById( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, propertyTypeId.toString() );
    }

    @Override
    public boolean deleteApp( UUID appId ) {
        return deleteObjectById( APP_INDEX, APP, appId.toString() );
    }

    @Override
    public boolean deleteAppType( UUID appTypeId ) {
        return deleteObjectById( APP_TYPE_INDEX, APP_TYPE, appTypeId.toString() );
    }

    @Override
    public boolean updateEntitySetMetadata( EntitySet entitySet ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        Map<String, Object> entitySetObj = Maps.newHashMap();
        entitySetObj.put( ENTITY_SET, entitySet );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( entitySetObj );
            UpdateRequest updateRequest = new UpdateRequest(
                    ENTITY_SET_DATA_MODEL,
                    ENTITY_SET_TYPE,
                    entitySet.getId().toString() ).doc( s, XContentType.JSON );
            client.update( updateRequest ).actionGet();
            return true;
        } catch ( IOException e ) {
            logger.debug( "error updating entity set metadata in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> updatedPropertyTypes ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        Map<String, Object> propertyTypes = Maps.newHashMap();
        propertyTypes.put( PROPERTY_TYPES, updatedPropertyTypes );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( propertyTypes );
            UpdateRequest updateRequest = new UpdateRequest(
                    ENTITY_SET_DATA_MODEL,
                    ENTITY_SET_TYPE,
                    entitySetId.toString() ).doc( s, XContentType.JSON );
            client.update( updateRequest ).actionGet();
            return true;
        } catch ( IOException e ) {
            logger.debug( "error updating property types of entity set in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean createOrganization( Organization organization ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        Map<String, Object> organizationObject = Maps.newHashMap();
        organizationObject.put( ID, organization.getId() );
        organizationObject.put( TITLE, organization.getTitle() );
        organizationObject.put( DESCRIPTION, organization.getDescription() );
        UUID organizationId = organization.getSecurablePrincipal().getId();
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( organizationObject );
            client.prepareIndex( ORGANIZATIONS, ORGANIZATION_TYPE, organizationId.toString() )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating organization in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean deleteOrganization( UUID organizationId ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        client.prepareDelete( ORGANIZATIONS, ORGANIZATION_TYPE, organizationId.toString() ).execute().actionGet();

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE ).filter(
                QueryBuilders.boolQuery()
                        .must( QueryBuilders.matchQuery( TYPE_FIELD, ACLS ) )
                        .must( QueryBuilders.matchQuery( ORGANIZATION_ID, organizationId.toString() ) ) )
                .source( ORGANIZATIONS )
                .execute()
                .actionGet();

        return true;
    }

    /*** METADATA SEARCHES ***/

    @Override
    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.EntityType );
        return executeSearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override
    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.AssociationType );
        return executeSearch( ASSOCIATION_TYPE_INDEX, ASSOCIATION_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override
    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.PropertyTypeInEntitySet );
        return executeSearch( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.App );
        return executeSearch( APP_INDEX, APP, searchTerm, start, maxHits, fieldsMap );
    }

    @Override public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.AppType );
        return executeSearch( APP_TYPE_INDEX, APP_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override
    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        return executeFQNSearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, namespace, name, start, maxHits );

    }

    @Override
    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        return executeFQNSearch( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, namespace, name, start, maxHits );
    }

    @Override
    public SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits ) {
        if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }

        BoolQueryBuilder query = new BoolQueryBuilder()
                .should( QueryBuilders.matchQuery( TITLE, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( DESCRIPTION, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .minimumShouldMatch( 1 );

        query.filter( QueryBuilders.idsQuery()
                .addIds( authorizedOrganizationIds.stream().map( aclKey -> aclKey.get( 0 ).toString() )
                        .toArray( String[]::new ) ) );

        SearchResponse response = client.prepareSearch( ORGANIZATIONS )
                .setTypes( ORGANIZATION_TYPE )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            Map<String, Object> hitMap = hit.getSourceAsMap();
            hitMap.put( "id", hit.getId() );
            hits.add( hitMap );
        }

        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public SearchResult executeEntitySetMetadataSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits ) {
        if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }

        BoolQueryBuilder query = new BoolQueryBuilder();

        if ( optionalSearchTerm.isPresent() ) {
            String searchTerm = optionalSearchTerm.get();
            Map<String, Float> fieldsMap = Maps.newHashMap();
            fieldsMap.put( ENTITY_SET + "." + ID, 1F );
            fieldsMap.put( ENTITY_SET + "." + NAME, 1F );
            fieldsMap.put( ENTITY_SET + "." + TITLE, 1F );
            fieldsMap.put( ENTITY_SET + "." + DESCRIPTION, 1F );

            query.must( QueryBuilders.queryStringQuery( getFormattedFuzzyString( searchTerm ) ).fields( fieldsMap )
                    .lenient( true ).fuzziness( Fuzziness.AUTO ) );
        }

        if ( optionalEntityType.isPresent() ) {
            UUID eid = optionalEntityType.get();
            query.must( QueryBuilders.matchQuery( ENTITY_SET + "." + ENTITY_TYPE_ID, eid.toString() ) );
        } else if ( optionalPropertyTypes.isPresent() ) {
            Set<UUID> propertyTypes = optionalPropertyTypes.get();
            for ( UUID pid : propertyTypes ) {
                query.must( QueryBuilders.nestedQuery( PROPERTY_TYPES,
                        QueryBuilders.matchQuery( PROPERTY_TYPES + "." + ID, pid.toString() ),
                        ScoreMode.Avg ) );
            }
        }

        query.filter( QueryBuilders.idsQuery()
                .addIds( authorizedAclKeys.stream().map( aclKey -> aclKey.get( 0 ).toString() )
                        .toArray( String[]::new ) ) );
        SearchResponse response = client.prepareSearch( ENTITY_SET_DATA_MODEL )
                .setTypes( ENTITY_SET_TYPE )
                .setQuery( query )
                .setFetchSource( new String[] { ENTITY_SET, PROPERTY_TYPES }, null )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        response.getHits().forEach( hit -> hits.add( hit.getSourceAsMap() ) );
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    /*** RE-INDEXING ***/

    @Override
    public boolean triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        Function<Object, String> idFn = pt -> ( (PropertyType) pt ).getId().toString();
        return triggerIndex( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, propertyTypes, idFn );
    }

    @Override
    public boolean triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        Function<Object, String> idFn = et -> ( (EntityType) et ).getId().toString();
        return triggerIndex( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityTypes, idFn );
    }

    @Override
    public boolean triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        Function<Object, String> idFn = at -> ( (AssociationType) at ).getAssociationEntityType().getId().toString();
        return triggerIndex( ASSOCIATION_TYPE_INDEX, ASSOCIATION_TYPE, associationTypes, idFn );
    }

    @Override
    public boolean triggerEntitySetIndex(
            Map<EntitySet, Set<UUID>> entitySets,
            Map<UUID, PropertyType> propertyTypes ) {
        Function<Object, String> idFn = map -> ( (Map<String, EntitySet>) map ).get( ENTITY_SET ).getId().toString();

        List<Map<String, Object>> entitySetMaps = entitySets.entrySet().stream().map( entry -> {
            Map<String, Object> entitySetMap = Maps.newHashMap();
            entitySetMap.put( ENTITY_SET, entry.getKey() );
            entitySetMap.put( PROPERTY_TYPES,
                    entry.getValue().stream().map( id -> propertyTypes.get( id ) ).collect( Collectors.toList() ) );
            return entitySetMap;
        } ).collect( Collectors.toList() );

        return triggerIndex( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetMaps, idFn );
    }

    @Override
    public boolean triggerAppIndex( List<App> apps ) {
        Function<Object, String> idFn = app -> ( (App) app ).getId().toString();
        return triggerIndex( APP_INDEX, APP, apps, idFn );
    }

    @Override
    public boolean triggerAppTypeIndex( List<AppType> appTypes ) {
        Function<Object, String> idFn = at -> ( (AppType) at ).getId().toString();
        return triggerIndex( APP_TYPE_INDEX, APP_TYPE, appTypes, idFn );
    }

    @Override
    public boolean triggerOrganizationIndex( List<Organization> organizations ) {
        Function<Object, String> idFn = org -> ( (Map<String, Object>) org ).get( ID ).toString();
        List<Map<String, Object>> organizationObjects =
                organizations.stream()
                        .map( organization -> {
                            Map<String, Object> organizationObject = Maps.newHashMap();
                            organizationObject.put( ID, organization.getId() );
                            organizationObject.put( TITLE, organization.getTitle() );
                            organizationObject.put( DESCRIPTION, organization.getDescription() );
                            return organizationObject;
                        } )
                        .collect( Collectors.toList() );

        return triggerIndex( ORGANIZATIONS, ORGANIZATION_TYPE, organizationObjects, idFn );
    }

    @Override
    public boolean clearAllData() {
        client.admin().indices()
                .delete( new DeleteIndexRequest( DATA_INDEX_PREFIX + "*" ) );
        DeleteByQueryAction.INSTANCE.newRequestBuilder( client )
                .filter( QueryBuilders.matchAllQuery() ).source( ENTITY_SET_DATA_MODEL,
                ENTITY_TYPE_INDEX,
                PROPERTY_TYPE_INDEX,
                ASSOCIATION_TYPE_INDEX,
                ORGANIZATIONS,
                APP_INDEX,
                APP_TYPE_INDEX )
                .get();
        return true;
    }


    /* HELPERS */

    private String getFormattedFuzzyString( String searchTerm ) {
        return Stream.of( searchTerm.split( " " ) )
                .map( term -> term.endsWith( "~" ) || term.endsWith( "\"" ) ? term : term + "~" )
                .collect( Collectors.joining( " " ) );
    }

    private boolean saveObjectToElasticsearch( String index, String type, Object obj, String id ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( obj );
            client.prepareIndex( index, type, id )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving object to elasticsearch" );
        }
        return false;
    }

    private boolean deleteObjectById( String index, String type, String id ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        client.prepareDelete( index, type, id ).execute().actionGet();
        return true;
    }

    private SearchResult executeSearch(
            String index,
            String type,
            String searchTerm,
            int start,
            int maxHits,
            Map<String, Float> fieldsMap ) {
        if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }

        QueryBuilder query = QueryBuilders.queryStringQuery( getFormattedFuzzyString( searchTerm ) ).fields( fieldsMap )
                .lenient( true );

        SearchResponse response = client.prepareSearch( index )
                .setTypes( type )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSourceAsMap() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    private SearchResult executeFQNSearch(
            String index,
            String type,
            String namespace,
            String name,
            int start,
            int maxHits ) {
        if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.must( QueryBuilders.regexpQuery( TYPE + "." + NAMESPACE, ".*" + namespace + ".*" ) )
                .must( QueryBuilders.regexpQuery( TYPE + "." + NAME, ".*" + name + ".*" ) );

        SearchResponse response = client.prepareSearch( index )
                .setTypes( type )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSourceAsMap() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    private Map<String, Float> getFieldsMap( SecurableObjectType objectType ) {
        float f = 1F;
        Map<String, Float> fieldsMap = Maps.newHashMap();

        List<String> fields = Lists.newArrayList( TITLE, DESCRIPTION );
        switch ( objectType ) {
            case AssociationType: {
                fields.add( ENTITY_TYPE_FIELD + "." + TYPE + "." + NAME );
                fields.add( ENTITY_TYPE_FIELD + "." + TYPE + "." + NAMESPACE );
                break;
            }

            case App: {
                fields.add( NAME );
                fields.add( URL );
                break;
            }

            default: {
                fields.add( TYPE + "." + NAME );
                fields.add( TYPE + "." + NAMESPACE );
                break;
            }
        }

        fields.forEach( field -> fieldsMap.put( field, f ) );
        return fieldsMap;
    }

    public boolean triggerIndex(
            String index,
            String type,
            Iterable<? extends Object> objects,
            Function<Object, String> idFn ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        BoolQueryBuilder deleteQuery = QueryBuilders.boolQuery();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        client.delete( new DeleteRequest( index ) ).actionGet();
        createIndex( index );

        objects.forEach( object -> {
            try {
                String id = idFn.apply( object );
                String s = ObjectMappers.getJsonMapper().writeValueAsString( object );
                deleteQuery.mustNot( QueryBuilders.matchQuery( "_id", id ) );
                bulkRequest
                        .add( client.prepareIndex( index, type, id )
                                .setSource( s, XContentType.JSON ) );
            } catch ( JsonProcessingException e ) {
                logger.debug( "Error re-indexing securable object types" );
            }
        } );

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE )
                .filter( deleteQuery )
                .source( index )
                .execute()
                .actionGet();

        BulkResponse bulkResponse = bulkRequest.get();
        if ( bulkResponse.hasFailures() ) {
            bulkResponse.forEach( item -> logger
                    .debug( "Failure during attempted re-index: {}", item.getFailureMessage() ) );
        }

        return true;
    }

    public boolean verifyElasticsearchConnection() {
        if ( connected ) {
            if ( !factory.isConnected( client ) ) {
                connected = false;
            }
        } else {
            client = factory.getClient();
            if ( client != null ) {
                connected = true;
            }
        }
        return connected;
    }

    @Scheduled(
            fixedRate = 1800000 )
    public void verifyRunner() throws UnknownHostException {
        verifyElasticsearchConnection();
    }

}
