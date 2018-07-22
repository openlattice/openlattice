package com.openlattice.datastore.services;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.neuron.audit.AuditEntitySetUtils;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class BackgroundIndexingService {
    private static final Logger logger     = LoggerFactory.getLogger( BackgroundIndexingService.class );
    private static final int    BLOCK_SIZE = 10000;

    private final HikariDataSource               hds;
    private final PostgresEntityDataQueryService dataQueryService;
    private final ConductorElasticsearchApi      elasticsearchApi;

    private final IMap<UUID, PropertyType> propertyTypes;
    private final IMap<UUID, EntityType>   entityTypes;
    private final IMap<UUID, EntitySet>    entitySets;

    public BackgroundIndexingService(
            HikariDataSource hds,
            HazelcastInstance hazelcastInstance,
            ConductorElasticsearchApi elasticsearchApi,
            PostgresEntityDataQueryService dataQueryService ) {
        this.hds = hds;
        this.dataQueryService = dataQueryService;
        this.elasticsearchApi = elasticsearchApi;

        this.propertyTypes = hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() );
        this.entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );

        indexUpdatedEntitySets();

    }

    private String getDirtyEntitiesQuery( UUID entitySetId ) {
        return "SELECT * FROM " + DataTables.entityTableName( entitySetId ) + " WHERE "
                + PostgresColumn.LAST_INDEX_FIELD + " < " + PostgresColumn.LAST_WRITE_FIELD + " LIMIT " + String
                .valueOf( BLOCK_SIZE )
                + " OFFSET ?";
    }

    private Set<UUID> getDirtyEntityKeyIds( UUID entitySetId ) {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getDirtyEntitiesQuery( entitySetId ) ) ) {

            ps.setFetchSize( BLOCK_SIZE );
            ResultSet rs = ps.executeQuery();

            Set<UUID> result = Sets.newHashSet();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.id( rs ) );
            }
            rs.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load all entity set ids", e );
            return ImmutableSet.of();
        }
    }

    private Map<UUID, Map<UUID, PropertyType>> getPropertyTypesByEntityTypesById() {
        Map<UUID, Map<UUID, PropertyType>> result = Maps.newHashMap();

        entityTypes.entrySet().forEach( entry -> {
            UUID entityTypeId = entry.getKey();
            Map<UUID, PropertyType> propertyTypesById = propertyTypes
                    .getAll( entityTypes.get( entityTypeId ).getProperties() ).entrySet().stream()
                    .filter( ptEntry -> !ptEntry.getValue().getDatatype().equals( EdmPrimitiveTypeKind.Binary ) )
                    .collect(
                            Collectors.toMap( ptEntry -> ptEntry.getKey(), ptEntry -> ptEntry.getValue() ) );
            result.put( entityTypeId, propertyTypesById );
        } );

        return result;
    }

    @Scheduled( fixedRate = 500 )
    public void indexUpdatedEntitySets() {

        Map<UUID, Map<UUID, PropertyType>> propertyTypesByEntityType = getPropertyTypesByEntityTypesById();

        entitySets.values().forEach( entitySet -> {
            if ( !entitySet.getName().equals( AuditEntitySetUtils.AUDIT_ENTITY_SET_NAME ) ) {

                UUID entitySetId = entitySet.getId();
                Map<UUID, PropertyType> propertyTypeMap = propertyTypesByEntityType
                        .get( entitySet.getEntityTypeId() );

                Set<UUID> entityKeyIdsToIndex = getDirtyEntityKeyIds( entitySetId );
                while ( entityKeyIdsToIndex.size() > 0 ) {

                    Map<UUID, SetMultimap<UUID, Object>> entitiesById = dataQueryService
                            .getEntitiesById( entitySetId, propertyTypeMap, entityKeyIdsToIndex );

                    elasticsearchApi.createBulkEntityData( entitySetId, entitiesById );

                    entityKeyIdsToIndex = getDirtyEntityKeyIds( entitySetId );
                }

            }
        } );

    }

}
