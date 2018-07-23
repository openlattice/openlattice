package com.openlattice.indexing;

import static com.openlattice.postgres.DataTables.quote;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class BackgroundIndexingService {
    private static final Logger logger     = LoggerFactory.getLogger( BackgroundIndexingService.class );
    private static final int    INDEX_SIZE = 32000;
    private static final int    FETCH_SIZE = 128000;

    private final HikariDataSource               hds;
    private final PostgresEntityDataQueryService dataQueryService;
    private final ConductorElasticsearchApi      elasticsearchApi;

    private final IMap<UUID, PropertyType> propertyTypes;
    private final IMap<UUID, EntityType>   entityTypes;
    private final IMap<UUID, EntitySet>    entitySets;
    private final IAtomicLong              totalIndexed;
    private final Semaphore                backgroundLimiter;

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

        totalIndexed = hazelcastInstance
                .getAtomicLong( "com.openlattice.datastore.services.BackgroundIndexingService" );
        backgroundLimiter = new Semaphore( Math.max( 1, Runtime.getRuntime().availableProcessors() / 2 ) );
    }

    private String getDirtyEntitiesQuery( UUID entitySetId ) {
        return "SELECT * FROM " + quote(DataTables.entityTableName( entitySetId )) + " WHERE "
                + PostgresColumn.LAST_INDEX_FIELD + " < " + PostgresColumn.LAST_WRITE_FIELD + " LIMIT "  + FETCH_SIZE;
    }

    private PostgresIterable<UUID> getDirtyEntityKeyIds( UUID entitySetId ) {
        return new PostgresIterable<>(
                () -> {
                    Connection connection;
                    PreparedStatement ps;
                    ResultSet rs;
                    try {
                        connection = hds.getConnection();
                        ps = connection.prepareStatement( getDirtyEntitiesQuery( entitySetId ) );
//                        ps.setFetchSize( FETCH_SIZE );
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
        final Map<UUID, Map<UUID, PropertyType>> result = Maps.newHashMap();

        entityTypes.entrySet().forEach( entry -> {
            final UUID entityTypeId = entry.getKey();
            final Map<UUID, PropertyType> propertyTypesById = propertyTypes
                    .getAll( entityTypes.get( entityTypeId ).getProperties() ).entrySet().stream()
                    .filter( ptEntry -> !ptEntry.getValue().getDatatype().equals( EdmPrimitiveTypeKind.Binary ) )
                    .collect(
                            Collectors.toMap( ptEntry -> ptEntry.getKey(), ptEntry -> ptEntry.getValue() ) );
            result.put( entityTypeId, propertyTypesById );
        } );

        return result;
    }

    @Scheduled( fixedRate = 30000, initialDelay = 180000 )
    public void indexUpdatedEntitySets() {
        Stopwatch w = Stopwatch.createStarted();

        final Map<UUID, Map<UUID, PropertyType>> propertyTypesByEntityType = getPropertyTypesByEntityTypesById();

        entitySets.values().parallelStream().forEach( entitySet -> {
            if ( backgroundLimiter.tryAcquire() && entitySets.tryLock( entitySet.getId() ) ) {
                try {
                    logger.info( "Starting indexing for entity set {} with id {}",
                            entitySet.getName(),
                            entitySet.getId() );

                    if ( !entitySet.getName().equals( AuditEntitySetUtils.AUDIT_ENTITY_SET_NAME ) ) {
                        Stopwatch esw = Stopwatch.createStarted();
                        final UUID entitySetId = entitySet.getId();
                        final Map<UUID, PropertyType> propertyTypeMap = propertyTypesByEntityType
                                .get( entitySet.getEntityTypeId() );

                        final PostgresIterable<UUID> entityKeyIdsToIndex = getDirtyEntityKeyIds( entitySetId );
                        final PostgresIterable.PostgresIterator<UUID> toIndexIter = entityKeyIdsToIndex.iterator();
                        int indexCount = 0;
                        while ( toIndexIter.hasNext() ) {
                            var batchToIndex = getBatch( toIndexIter );
                            Map<UUID, SetMultimap<UUID, Object>> entitiesById = dataQueryService
                                    .getEntitiesById( entitySetId, propertyTypeMap, batchToIndex );

                            if ( elasticsearchApi.createBulkEntityData( entitySetId, entitiesById ) ) {
                                indexCount += dataQueryService.markAsIndexed( entitySetId, batchToIndex );
                                logger.info( "Indexed batch of {} elements for {} ({}) in {} ms",
                                        indexCount,
                                        entitySet.getName(),
                                        entitySet.getId(),
                                        esw.elapsed( TimeUnit.MILLISECONDS ) );
                            }
                        }
                        logger.info( "Finished indexing entity set {} in {} ms",
                                entitySet.getName(),
                                esw.elapsed( TimeUnit.MILLISECONDS ) );
                        logger.info( "Indexed {} elements in {} ms",
                                totalIndexed.addAndGet( indexCount ),
                                w.elapsed( TimeUnit.MILLISECONDS ) );
                    }
                } finally {
                    entitySets.unlock( entitySet.getId() );
                    backgroundLimiter.release();
                }
                logger.info( "Indexed total number of {} elements in {} ms",
                        totalIndexed.get(),
                        w.elapsed( TimeUnit.MILLISECONDS ) );
            } else {
                logger.info("Skipping indexing as thread limit hit.");
            }
        } );

    }

    private static Set<UUID> getBatch( Iterator<UUID> entityKeyIdStream ) {
        var entityKeyIds = new HashSet<UUID>( INDEX_SIZE );

        for ( int i = 0; i < INDEX_SIZE && entityKeyIdStream.hasNext(); ++i ) {
            entityKeyIds.add( entityKeyIdStream.next() );
        }
        return entityKeyIds;
    }

}
