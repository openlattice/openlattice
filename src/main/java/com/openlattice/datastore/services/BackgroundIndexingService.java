package com.openlattice.datastore.services;

import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
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
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.postgres.streams.StatementHolder;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class BackgroundIndexingService {
    private static final Logger logger     = LoggerFactory.getLogger( BackgroundIndexingService.class );
    private static final int    BLOCK_SIZE = 10000;

    private final HikariDataSource               hds;
    private final PostgresEntityDataQueryService dataQueryService;
    private final ConductorElasticsearchApi      elasticsearchApi;

    private final IMap<UUID, PropertyType> propertyTypes;
    private final IMap<UUID, EntityType>   entityTypes;
    private final IMap<UUID, EntitySet>    entitySets;
    private final ILock                    lock;

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

        lock = hazelcastInstance.getLock( "com.openlattice.datastore.services.BackgroundIndexingService" );
        indexUpdatedEntitySets();
    }

    private String getDirtyEntitiesQuery( UUID entitySetId ) {
        return "SELECT * FROM " + DataTables.entityTableName( entitySetId ) + " WHERE "
                + PostgresColumn.LAST_INDEX_FIELD + " < " + PostgresColumn.LAST_WRITE_FIELD;
    }

    private PostgresIterable<UUID> getDirtyEntityKeyIds( UUID entitySetId ) {
        return new PostgresIterable<>(
                () -> {
                    Connection connection = null;
                    PreparedStatement ps;
                    ResultSet rs;
                    try {
                        connection = hds.getConnection();
                        ps = connection.prepareStatement( getDirtyEntitiesQuery( entitySetId ) );
                        ps.setFetchSize( BLOCK_SIZE );
                        rs = ps.executeQuery();
                        return new StatementHolder( connection, ps, rs );
                    } catch ( SQLException e ) {
                        logger.error( "Unable to instantiate postgres iterable.", e );
                        throw new IllegalStateException( "Unable to instantiate postgres iterable.", e );
                    }
                },
                resultSet -> {
                    try {
                        return ResultSetAdapters.id( resultSet );
                    } catch ( SQLException e ) {
                        logger.error( "Unable to retrieve postgres value", e );
                        throw new IllegalStateException( "Unable to instantiate postgres iterable.", e );
                    }
                }
        );
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
        try {
            lock.lock();
            Map<UUID, Map<UUID, PropertyType>> propertyTypesByEntityType = getPropertyTypesByEntityTypesById();

            entitySets.values().forEach( entitySet -> {
                if ( !entitySet.getName().equals( AuditEntitySetUtils.AUDIT_ENTITY_SET_NAME ) ) {

                    UUID entitySetId = entitySet.getId();
                    Map<UUID, PropertyType> propertyTypeMap = propertyTypesByEntityType
                            .get( entitySet.getEntityTypeId() );

                    PostgresIterable<UUID> entityKeyIdsToIndex = getDirtyEntityKeyIds( entitySetId );

                    PostgresIterable.PostgresIterator<UUID> toIndexIter = entityKeyIdsToIndex.iterator();

                    while ( toIndexIter.hasNext() ) {
                        var batchToIndex = getBatch( toIndexIter );
                        Map<UUID, SetMultimap<UUID, Object>> entitiesById = dataQueryService
                                .getEntitiesById( entitySetId, propertyTypeMap, batchToIndex );

                        if ( elasticsearchApi.createBulkEntityData( entitySetId, entitiesById ) ) {
                            dataQueryService.markAsIndexed( entitySetId, batchToIndex );
                        }
                    }
                }
            } );
        } finally {
            lock.unlock();
        }
    }

    private static Set<UUID> getBatch( Iterator<UUID> entityKeyIdStream ) {
        var entityKeyIds = new HashSet<UUID>( BLOCK_SIZE );

        for ( int i = 0; i < BLOCK_SIZE && entityKeyIdStream.hasNext(); ++i ) {
            entityKeyIds.add( entityKeyIdStream.next() );
        }
        return entityKeyIds;
    }

}
