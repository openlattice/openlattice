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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConductorElasticsearchImpl implements ConductorElasticsearchApi {
    // @formatter:off
    private final int MAX_CONCURRENT_SEARCHES = 3;

    private static final ObjectMapper mapper = ObjectMappers.newJsonMapper();
    private static final Logger       logger = LoggerFactory
            .getLogger( ConductorElasticsearchImpl.class );

    static {
        mapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
    }

    private final MultiLayerNetwork                   net;
    private final ThreadLocal                         modelThread;
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
            Client someClient ) {
        this( config, Optional.of( someClient ) );
    }

    public ConductorElasticsearchImpl(
            SearchConfiguration config,
            Optional<Client> someClient ) {
        init( config );
        client = someClient.orElseGet( factory::getClient );
        initializeIndices();

        MultiLayerNetwork network;
        try {
            network = ModelSerializer
                    .restoreMultiLayerNetwork(
                            Thread.currentThread().getContextClassLoader().getResourceAsStream( "model.bin" ) );
        } catch ( IOException e ) {
            network = null;
            logger.error( "Unable to load neural net", e );
        }
        this.net = checkNotNull( network );

        modelThread = ThreadLocal.withInitial( net::clone );
    }

    private void init( SearchConfiguration config ) {
        server = config.getElasticsearchUrl();
        cluster = config.getElasticsearchCluster();
        port = config.getElasticsearchPort();
        factory = new ElasticsearchTransportClientFactory( server, port, cluster );
    }

    public void initializeIndices() {
        initializeEntitySetDataModelIndex();
        initializeOrganizationIndex();
        initializeEntityTypeIndex();
        initializeAssociationTypeIndex();
        initializePropertyTypeIndex();
        initializeAppIndex();
        initializeAppTypeIndex();
    }

    @Override
    public Set<UUID> getEntitySetWithIndices() {
        return Stream.of( client.admin().indices().prepareGetIndex().setFeatures().get().getIndices() )
                .filter( s -> s.startsWith( SECURABLE_OBJECT_INDEX_PREFIX ) )
                .map( s -> UUID.fromString( s.substring( SECURABLE_OBJECT_INDEX_PREFIX.length() ) ) )
                .collect( Collectors.toSet() );
    }

    // @formatter:off
    private XContentBuilder getMetaphoneSettings() throws IOException {
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
                	            .field( TOKENIZER, STANDARD )
                	            .field( FILTER, Lists.newArrayList( STANDARD, LOWERCASE, SHINGLE_FILTER, METAPHONE_FILTER ) )
                	        .endObject()
                	    .endObject()
        	        .endObject()
        	        .field( NUM_SHARDS, 3 )
        	        .field( NUM_REPLICAS, 3 )
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
        entitySetData.put( ES_PROPERTIES, properties );
        mapping.put( ENTITY_SET_TYPE, entitySetData );

        client.admin().indices().prepareCreate( ENTITY_SET_DATA_MODEL )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ENTITY_SET_TYPE, mapping )
                .execute().actionGet();
        return true;
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
        organizationData.put( ES_PROPERTIES, properties );
        organizationMapping.put( ORGANIZATION_TYPE, organizationData );

        client.admin().indices().prepareCreate( ORGANIZATIONS )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
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
                        .put( NUM_SHARDS, 3 )
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
                        .put( NUM_SHARDS, 3 )
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
                        .put( NUM_SHARDS, 3 )
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
                        .put( NUM_SHARDS, 3 )
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
                        .put( NUM_SHARDS, 3 )
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
            case Byte: {
                fieldMapping.put( TYPE, BYTE );
                break;
            }
            case SByte: {
                fieldMapping.put( TYPE, BYTE );
                break;
            }
            case Decimal: {
                fieldMapping.put( TYPE, FLOAT );
                break;
            }
            case Single: {
                fieldMapping.put( TYPE, DOUBLE );
                break;
            }
            case Double: {
                fieldMapping.put( TYPE, DOUBLE );
                break;
            }
            case Guid: {
                fieldMapping.put( TYPE, KEYWORD );
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
            default: {
                fieldMapping.put( INDEX, "false" );
                fieldMapping.put( TYPE, KEYWORD );
            }
        }
        return fieldMapping;
    }

    private String getIndexName( UUID securableObjectId ) {
        return SECURABLE_OBJECT_INDEX_PREFIX + securableObjectId.toString();
    }

    private String getTypeName( UUID securableObjectId ) {
        return SECURABLE_OBJECT_TYPE_PREFIX + securableObjectId.toString();
    }

    @Override
    public boolean createSecurableObjectIndex(
            UUID entitySetId,
            List<PropertyType> propertyTypes,
            Optional<Set<UUID>> linkedEntitySetIds ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        String indexName = getIndexName( entitySetId );
        String typeName = getTypeName( entitySetId );

        boolean exists = client.admin().indices()
                .prepareExists( indexName ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        // securable_object_row type mapping
        Map<String, Object> securableObjectData = Maps.newHashMap();
        Map<String, Object> securableObjectMapping = Maps.newHashMap();
        Map<String, Object> properties = Maps.newHashMap();

        if ( linkedEntitySetIds.isPresent() ) {
            linkedEntitySetIds.get().forEach( ( linkedEntitySetId ) -> {
                for ( PropertyType propertyType : propertyTypes ) {
                    if ( !propertyType.getDatatype().equals( EdmPrimitiveTypeKind.Binary ) ) {
                        properties.put(
                                linkedEntitySetId.toString() + "." + propertyType.getId().toString(),
                                getFieldMapping( propertyType ) );
                    }
                }
            } );
        } else {
            for ( PropertyType propertyType : propertyTypes ) {
                if ( !propertyType.getDatatype().equals( EdmPrimitiveTypeKind.Binary ) ) {
                    properties.put( propertyType.getId().toString(), getFieldMapping( propertyType ) );
                }
            }
        }

        securableObjectData.put( ES_PROPERTIES, properties );
        securableObjectMapping.put( typeName, securableObjectData );

        try {
            client.admin().indices().prepareCreate( indexName )
                    .setSettings( getMetaphoneSettings() )
                    .addMapping( typeName, securableObjectMapping )
                    .execute().actionGet();
        } catch ( IOException e ) {
            logger.debug( "unable to create securable object index" );
        }
        return true;
    }

    @Override
    public boolean saveEntitySetToElasticsearch(
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
            createSecurableObjectIndex(
                    entitySet.getId(),
                    propertyTypes,
                    ( entitySet.isLinking() ) ? Optional.of( entitySet.getLinkedEntitySets() ) : Optional.empty() );
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving entity set to elasticsearch" );
        }
        return false;
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

        BoolQueryBuilder authorizedFilterQuery = new BoolQueryBuilder();
        for ( AclKey aclKey : authorizedAclKeys ) {
            authorizedFilterQuery
                    .should( QueryBuilders.matchQuery( ENTITY_SET + "." + ID, aclKey.get( 0 ).toString() ) );
        }
        authorizedFilterQuery.minimumShouldMatch( 1 );

        BoolQueryBuilder query = new BoolQueryBuilder().must( authorizedFilterQuery );

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
        SearchResponse response = client.prepareSearch( ENTITY_SET_DATA_MODEL )
                .setTypes( ENTITY_SET_TYPE )
                .setQuery( query )
                .setFetchSource( new String[]{ ENTITY_SET, PROPERTY_TYPES }, null )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        response.getHits().forEach( hit -> hits.add( hit.getSourceAsMap() ) );
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        Map<String, Object> propertyTypes = Maps.newHashMap();
        propertyTypes.put( PROPERTY_TYPES, newPropertyTypes );
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
    public boolean addLinkedEntitySetsToEntitySet( UUID linkingEntitySetId,
                                                   List<PropertyType> propertyTypes,
                                                   Set<UUID> newLinkedEntitySets ) {
        return createSecurableObjectIndex( linkingEntitySetId, propertyTypes, Optional.of( newLinkedEntitySets ) );
    }

    @Override
    public boolean deleteEntitySet( UUID entitySetId ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        client.prepareDelete( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetId.toString() ).execute().actionGet();

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE ).filter(
                QueryBuilders.boolQuery()
                        .must( QueryBuilders.matchQuery( TYPE_FIELD, ACLS ) )
                        .must( QueryBuilders.matchQuery( ENTITY_SET_ID, entitySetId.toString() ) ) )
                .source( ENTITY_SET_DATA_MODEL )
                .execute()
                .actionGet();

        client.admin().indices()
                .delete( new DeleteIndexRequest( getIndexName( entitySetId ) ) );

        return true;
    }

    @Override
    public boolean clearEntitySetData( UUID entitySetId ) {
        if ( !verifyElasticsearchConnection() ) { return false; }

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE ).filter( QueryBuilders.matchAllQuery() )
                .source( getIndexName( entitySetId ) )
                .execute()
                .actionGet();

        return true;
    }

    @Override
    public Map<UUID, Set<UUID>> searchEntitySets(
            Iterable<UUID> entitySetIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        if ( !verifyElasticsearchConnection() ) { return null; }

        BoolQueryBuilder query = new BoolQueryBuilder();
        fieldSearches.entrySet().stream().forEach( entry -> {
            BoolQueryBuilder fieldQuery = new BoolQueryBuilder();
            entry.getValue().stream().forEach( searchTerm -> fieldQuery.should(
                    QueryBuilders.matchQuery( entry.getKey().toString(), searchTerm ).fuzziness( Fuzziness.AUTO )
                            .lenient( true ) ) );
            fieldQuery.minimumShouldMatch( 1 );
            query.should( fieldQuery );
        } );

        query.minimumShouldMatch( 1 );

        return StreamUtil.stream( entitySetIds )
                .parallel()
                .collect(
                        Collectors.toConcurrentMap( Function.identity(), entitySetId ->
                                Stream.of( client.prepareSearch( getIndexName( entitySetId ) )
                                        .setQuery( query )
                                        .setFrom( 0 )
                                        .setSize( size )
                                        .setExplain( explain )
                                        .execute()
                                        .actionGet().getHits().getHits() )
                                        .map( SearchHit::getId )
                                        .map( UUID::fromString )
                                        .collect( Collectors.toSet() ) ) );
    }

    private UUID getEntitySetIdFromIndex( String index ) {
        return UUID.fromString( index.substring( SECURABLE_OBJECT_INDEX_PREFIX.length() ) );
    }

    /**
     * Return a byte array of values with formatting the property ids(keys) to either {@literal <}propertyTypeId>
     * or {@literal <}entitySetId.propertyTypeId> depending on if it's a linking entity or not.
     *
     * @param entitySetIdOfEntities the entity set id for which the values belong
     * @param entityValues          map of values for an entity with property type id as key
     * @param linking               whether it is a linking entity or not
     * @return byte array of values
     * @throws JsonProcessingException
     */
    private byte[] getFieldMappings(
            UUID entitySetIdOfEntities,
            Map<UUID, Set<Object>> entityValues,
            boolean linking ) throws JsonProcessingException {
        if ( linking ) {
            String entitySetId = entitySetIdOfEntities.toString();
            Map<String, Set<Object>> formattedEntityValues = entityValues.entrySet().stream()
                    .collect( Collectors.toMap(
                            ( entry ) -> entitySetId + "." + entry.getKey().toString(),
                            Map.Entry::getValue
                    ) );

            return mapper.writeValueAsBytes( formattedEntityValues );
        } else {
            return mapper.writeValueAsBytes( entityValues );
        }
    }

    @Override
    public boolean createEntityData( EntityDataKey edk, Map<UUID, Set<Object>> propertyValues ) { //TODO linking
        if ( !verifyElasticsearchConnection() ) { return false; }

        UUID entitySetId = edk.getEntitySetId();
        UUID entityKeyId = edk.getEntityKeyId();

        try {
            String s = mapper.writeValueAsString( propertyValues );

            client.prepareIndex( getIndexName( entitySetId ), getTypeName( entitySetId ), entityKeyId.toString() )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();
        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating entity data in elasticsearch" );
            return false;
        }

        return true;
    }

    @Override
    public boolean createBulkEntityData(
            UUID entitySetId,
            Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByIdByEntitySetId,
            boolean linking ) {
        if ( !verifyElasticsearchConnection() ) { return false; }
        try {
            String indexName = getIndexName( entitySetId );
            String indexType = getTypeName( entitySetId );

            BulkRequestBuilder requestBuilder = client.prepareBulk();
            for ( Map.Entry<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByIdByEntitySetIdEntry : entitiesByIdByEntitySetId.entrySet() ) {
                final UUID entitySetIdOfEntities = entitiesByIdByEntitySetIdEntry.getKey();
                for ( Map.Entry<UUID, Map<UUID, Set<Object>>> entitiesByIdEntry : entitiesByIdByEntitySetIdEntry.getValue().entrySet() ) {
                    final UUID entityId = entitiesByIdEntry.getKey(); // either entityKeyId or linkingId
                    final byte[] s = getFieldMappings( entitySetIdOfEntities, entitiesByIdEntry.getValue(), linking );
                    requestBuilder.add(
                            client.prepareIndex( indexName, indexType, entityId.toString() )
                                    .setSource( s, XContentType.JSON ) );
                    //                requestBuilder.add( new IndexRequest( indexName, indexType, entityKeyId.toString() )
                    //                        .source( s, XContentType.JSON ) );
                }
            }
            requestBuilder.execute().actionGet();
        } catch ( JsonProcessingException e ) {
            logger.debug( "Error creating bulk entity data in elasticsearch for entity set {}", entitySetId, e );
            return false;
        }
        return true;
    }

    @Override
    public boolean updateEntityData( EntityDataKey edk, Map<UUID, Set<Object>> propertyValues ) {//TODO linking

        if ( !verifyElasticsearchConnection() ) { return false; }

        UUID entitySetId = edk.getEntitySetId();
        UUID entityKeyId = edk.getEntityKeyId();

        GetResponse result = client
                .prepareGet( getIndexName( entitySetId ), getTypeName( entitySetId ), entityKeyId.toString() ).get();
        if ( result.isExists() ) {
            result.getSourceAsMap().entrySet().forEach( entry ->
                    {
                        UUID propertyTypeId = UUID.fromString( entry.getKey() );
                        propertyValues.merge( propertyTypeId,
                                new HashSet<>( ( Collection<Object> ) entry.getValue() ),
                                ( first, second ) -> {
                                    first.addAll( second );
                                    return first;
                                } );
                    }
            );
        }

        return createEntityData( edk, propertyValues );
    }

    @Override
    public boolean deleteEntityData( EntityDataKey edk ) { //TODO linking
        if ( !verifyElasticsearchConnection() ) { return false; }

        UUID entitySetId = edk.getEntitySetId();
        UUID entityKeyId = edk.getEntityKeyId();

        client.prepareDelete( getIndexName( entitySetId ), getTypeName( entitySetId ), entityKeyId.toString() )
                .execute()
                .actionGet();
        return true;
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

    private EntityDataKeySearchResult getEntityDataKeySearchResult( SearchResponse response ) {
        Set<EntityDataKey> entityDataKeys = Sets.newHashSet();
        for ( SearchHit hit : response.getHits() ) {
            entityDataKeys.add( new EntityDataKey( getEntitySetIdFromIndex( hit.getIndex() ),
                    UUID.fromString( hit.getId() ) ) );
        }
        return new EntityDataKeySearchResult( response.getHits().getTotalHits(), entityDataKeys );
    }

    private EntityDataKeySearchResult getEntityDataKeySearchResult( MultiSearchResponse response ) {
        Set<EntityDataKey> entityDataKeys = Sets.newHashSet();
        var totalHits = 0;
        for ( MultiSearchResponse.Item item : response.getResponses() ) {
            for ( SearchHit hit : item.getResponse().getHits() ) {
                entityDataKeys.add( new EntityDataKey( getEntitySetIdFromIndex( hit.getIndex() ),
                        UUID.fromString( hit.getId() ) ) );
            }
            totalHits += item.getResponse().getHits().totalHits;
        }
        return new EntityDataKeySearchResult( totalHits, entityDataKeys );
    }

    /**
     * Creates a map with keys of all the property type ids puts 1 weights as value.
     */
    private static Map<String, Float> getFieldsMap( DelegatedUUIDSet authorizedPropertyTypes ) {
        Map<String, Float> fieldsMap = Maps.newHashMap();
        authorizedPropertyTypes.forEach( id -> fieldsMap.put( id.toString(), 1F ) );
        return fieldsMap;
    }

    /**
     * Creates a map with keys, that are the combinations of all the entity set ids and property type ids across the
     * input and values of 1 for weights.
     */
    private static Map<String, Float> getLinkingFieldsMap(
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet ) {
        Map<String, Float> fieldsMap = Maps.newHashMap();
        authorizedPropertyTypesByEntitySet.entrySet().stream()
                .forEach( authorizedPropertiesOfEntitySet -> authorizedPropertiesOfEntitySet.getValue().stream()
                        .forEach( propertyId ->
                                fieldsMap.put(
                                        authorizedPropertiesOfEntitySet.getKey().toString() + "." + propertyId.toString(),
                                        1F )
                        )
                );

        return fieldsMap;
    }

    private QueryBuilder getAdvancedSearchQuery( Constraint constraints, Set<UUID> authorizedProperties ) {

        BoolQueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch( 1 );
        constraints.getSearches().get().forEach( search -> {
            if ( authorizedProperties.contains( search.getPropertyType() ) ) {
                QueryStringQueryBuilder queryString = QueryBuilders
                        .queryStringQuery( search.getSearchTerm() )
                        .field( search.getPropertyType().toString(), 1F ).lenient( true );
                if ( search.getExactMatch() ) {
                    query.must( queryString );
                    query.minimumShouldMatch( 0 );
                } else { query.should( queryString ); }
            }
        } );

        return query;

    }

    private QueryBuilder getSimpleSearchQuery(
            Constraint constraints,
            Map<String, Float> fieldsMap ) {

        String searchTerm = constraints.getSearchTerm().get();
        boolean fuzzy = constraints.getFuzzy().get();

        String formattedSearchTerm = fuzzy ? getFormattedFuzzyString( searchTerm ) : searchTerm;

        return QueryBuilders.queryStringQuery( formattedSearchTerm )
                .fields( fieldsMap )
                .lenient( true );
    }

    private QueryBuilder getGeoDistanceSearchQuery( Constraint constraints, Set<UUID> authorizedProperties ) {

        UUID propertyTypeId = constraints.getPropertyTypeId().get();
        if ( !authorizedProperties.contains( propertyTypeId ) ) {
            return null;
        }

        double latitude = constraints.getLatitude().get();
        double longitude = constraints.getLongitude().get();
        double radius = constraints.getRadius().get();

        return QueryBuilders
                .geoDistanceQuery( propertyTypeId.toString() )
                .point( latitude, longitude )
                .distance( radius, DistanceUnit.fromString( constraints.getDistanceUnit().get().name() ) );
    }

    private QueryBuilder getGeoPolygonSearchQuery( Constraint constraints, Set<UUID> authorizedProperties ) {

        UUID propertyTypeId = constraints.getPropertyTypeId().get();
        if ( !authorizedProperties.contains( propertyTypeId ) ) {
            return null;
        }

        QueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch( 1 );

        for ( List<List<Double>> zone : constraints.getZones().get() ) {
            List<GeoPoint> polygon = zone.stream().map( pair -> new GeoPoint( pair.get( 1 ), pair.get( 0 ) ) )
                    .collect( Collectors.toList() );
            ( ( BoolQueryBuilder ) query ).should( QueryBuilders.geoPolygonQuery( propertyTypeId.toString(), polygon ) );
        }

        return query;
    }

    private QueryBuilder getQueryForSearch(
            SearchConstraints searchConstraints,
            Map<String, Float> fieldsMap,
            Set<UUID> authorizedProperties ) {
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        for ( ConstraintGroup constraintGroup : searchConstraints.getConstraintGroups() ) {
            BoolQueryBuilder subQuery = QueryBuilders.boolQuery()
                    .minimumShouldMatch( constraintGroup.getMinimumMatches() );

            for ( Constraint constraint : constraintGroup.getConstraints() ) {

                switch ( constraint.getSearchType() ) {
                    case advanced:
                        subQuery.should( getAdvancedSearchQuery( constraint, authorizedProperties ) );
                        break;

                    case geoDistance:
                        subQuery.should( getGeoDistanceSearchQuery( constraint, authorizedProperties ) );
                        break;

                    case geoPolygon:
                        subQuery.should( getGeoPolygonSearchQuery( constraint, authorizedProperties ) );
                        break;

                    case simple:
                        subQuery.should( getSimpleSearchQuery( constraint, fieldsMap ) );
                        break;

                }
            }

            query.must( subQuery );
        }

        return query;
    }

    @Override
    public EntityDataKeySearchResult executeSearch(
            SearchConstraints searchConstraints,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet,
            boolean linking ) {
        if ( !verifyElasticsearchConnection() ) {
            return new EntityDataKeySearchResult( 0, Sets.newHashSet() );
        }

        return ( linking ) ? executeLinkingSearch( searchConstraints, authorizedPropertyTypesByEntitySet ) :
                executeNormalSearch( searchConstraints, authorizedPropertyTypesByEntitySet );
    }

    private EntityDataKeySearchResult executeNormalSearch(
            SearchConstraints searchConstraints,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet ) {

        MultiSearchRequest requests = new MultiSearchRequest().maxConcurrentSearchRequests( MAX_CONCURRENT_SEARCHES );
        authorizedPropertyTypesByEntitySet.keySet().forEach(
                ( entitySetId ) -> {
                    Map<String, Float> fieldsMap = getFieldsMap( authorizedPropertyTypesByEntitySet.get( entitySetId ) );
                    QueryBuilder query = getQueryForSearch(
                            searchConstraints,
                            fieldsMap,
                            authorizedPropertyTypesByEntitySet.get( entitySetId ).unwrap() );

                    SearchRequestBuilder request = client
                            .prepareSearch( getIndexName( entitySetId ) )
                            .setQuery( query )
                            .setFrom( searchConstraints.getStart() )
                            .setSize( searchConstraints.getMaxHits() );
                    requests.add( request );
                }
        );

        MultiSearchResponse response = client.multiSearch( requests ).actionGet();
        return getEntityDataKeySearchResult( response );
    }

    private EntityDataKeySearchResult executeLinkingSearch(
            SearchConstraints searchConstraints,
            Map<UUID, DelegatedUUIDSet> authorizedPropertyTypesByEntitySet ) {
        Map<String, Float> fieldsMap = getLinkingFieldsMap( authorizedPropertyTypesByEntitySet );
        QueryBuilder query = getQueryForSearch(
                searchConstraints,
                fieldsMap,
                authorizedPropertyTypesByEntitySet.values().stream()
                        .flatMap( propertyIds -> propertyIds.unwrap().stream() )
                        .collect( Collectors.toSet() ) );
        if ( query == null ) {
            return new EntityDataKeySearchResult( 0, Sets.newHashSet() );
        }

        String[] indexNames = Arrays.stream( searchConstraints.getEntitySetIds() )
                .map( this::getIndexName ).toArray( String[]::new );

        SearchResponse response = client
                .prepareSearch( indexNames )
                .setQuery( query )
                .setFrom( searchConstraints.getStart() )
                .setSize( searchConstraints.getMaxHits() )
                .execute()
                .actionGet();

        return getEntityDataKeySearchResult( response );
    }

    @Override
    public SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits ) {
        if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }

        BoolQueryBuilder authorizedFilterQuery = new BoolQueryBuilder();
        for ( AclKey aclKey : authorizedOrganizationIds ) {
            authorizedFilterQuery.should( QueryBuilders.matchQuery( "_id", aclKey.get( 0 ).toString() ) );
        }
        authorizedFilterQuery.minimumShouldMatch( 1 );

        BoolQueryBuilder query = new BoolQueryBuilder().must( authorizedFilterQuery )
                .should( QueryBuilders.matchQuery( TITLE, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( DESCRIPTION, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .minimumShouldMatch( 1 );

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

    @Override
    public boolean saveEntityTypeToElasticsearch( EntityType entityType ) {
        return saveObjectToElasticsearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityType, entityType.getId().toString() );
    }

    @Override
    public boolean saveAssociationTypeToElasticsearch( AssociationType associationType ) {
        EntityType entityType = associationType.getAssociationEntityType();
        if ( entityType == null ) {
            logger.debug( "An association type must have an entity type present in order to save to elasticsearch" );
            return false;
        }

        return saveObjectToElasticsearch( ASSOCIATION_TYPE_INDEX,
                ASSOCIATION_TYPE,
                associationType,
                entityType.getId().toString() );
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
        return deleteObjectById( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityTypeId.toString() );
    }

    @Override
    public boolean deleteAssociationType( UUID associationTypeId ) {
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

    @Override
    public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.App );
        return executeSearch( APP_INDEX, APP, searchTerm, start, maxHits, fieldsMap );
    }

    @Override
    public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
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
    public boolean triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        Function<Object, String> idFn = pt -> ( ( PropertyType ) pt ).getId().toString();
        return triggerIndex( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, propertyTypes, idFn );
    }

    @Override
    public boolean triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        Function<Object, String> idFn = et -> ( ( EntityType ) et ).getId().toString();
        return triggerIndex( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityTypes, idFn );
    }

    @Override
    public boolean triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        Function<Object, String> idFn = at -> ( ( AssociationType ) at ).getAssociationEntityType().getId().toString();
        return triggerIndex( ASSOCIATION_TYPE_INDEX, ASSOCIATION_TYPE, associationTypes, idFn );
    }

    @Override
    public boolean triggerEntitySetIndex(
            Map<EntitySet, Set<UUID>> entitySets,
            Map<UUID, PropertyType> propertyTypes ) {
        Function<Object, String> idFn = map -> ( ( Map<String, EntitySet> ) map ).get( ENTITY_SET ).getId().toString();

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
        Function<Object, String> idFn = app -> ( ( App ) app ).getId().toString();
        return triggerIndex( APP_INDEX, APP, apps, idFn );
    }

    @Override
    public boolean triggerAppTypeIndex( List<AppType> appTypes ) {
        Function<Object, String> idFn = at -> ( ( AppType ) at ).getId().toString();
        return triggerIndex( APP_TYPE_INDEX, APP_TYPE, appTypes, idFn );
    }

    @Override
    public boolean triggerOrganizationIndex( List<Organization> organizations ) {
        Function<Object, String> idFn = org -> ( ( Map<String, Object> ) org ).get( ID ).toString();
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
                .delete( new DeleteIndexRequest( SECURABLE_OBJECT_INDEX_PREFIX + "*" ) );
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

    @Override
    public double getModelScore( double[][] features ) {
        return ( ( MultiLayerNetwork ) modelThread.get() ).output( Nd4j.create( features ) ).getDouble( 1 );
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

}
