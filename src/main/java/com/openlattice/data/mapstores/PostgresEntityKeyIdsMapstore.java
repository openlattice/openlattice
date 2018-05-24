/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.data.mapstores;

import static com.openlattice.postgres.PostgresColumn.ENTITY_ID;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.SYNC_ID;

import com.openlattice.data.EntityKey;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEntityKeyIdsMapstore extends AbstractBasePostgresMapstore<EntityKey, UUID> {
    public static final  String                 ID               = "this";
    private static final Logger                 logger           = LoggerFactory
            .getLogger( PostgresEntityKeyIdsMapstore.class );
    private static       Cache<EntityKey, UUID> entityKeyIdCache = CacheBuilder
            .newBuilder()
            .expireAfterAccess( 5, TimeUnit.SECONDS )
            .build();

    //10 UUID collisions per second or failed writes per second means something bad is happening
    private final RateLimiter rateLimiter = RateLimiter.create( 10 );

    public PostgresEntityKeyIdsMapstore( HikariDataSource hds ) throws SQLException {
        super( HazelcastMap.IDS.name(), PostgresTable.IDS, hds );
    }

    @Override protected Optional<String> buildOnConflictQuery() {
        return Optional.empty();
    }

    @Override
    public EntityKey generateTestKey() {
        return TestDataFactory.entityKey();
    }

    @Override
    public UUID generateTestValue() {
        return UUID.randomUUID();
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig()
                .setInitialLoadMode( InitialLoadMode.EAGER )
                .setWriteDelaySeconds( 5 );
    }

    @Override
    public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_ENTITY_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_SYNC_ID, false ) );
    }

    @Override
    public void store( EntityKey key, UUID value ) {
        //The problem is that a failed write will lead to an unexpected value being written.
        //Code shouldn't be setting EntityKeyIds directly anyway.
        //TODO: Disable after migration
        throw new UnsupportedOperationException( "Directly writing entity key id is not supported." );
        //        try ( Connection connection = hds.getConnection(); PreparedStatement insertRow = prepareInsert( connection ) ) {
        //            bind( insertRow, key, value );
        //            logger.info( insertRow.toString() );
        //            insertRow.execute();
        //        } catch ( SQLException e ) {
        //            logger.error( "Error executing SQL during store for key {}.", key, e );
        //        }
    }

    @Override
    public void storeAll( Map<EntityKey, UUID> map ) {
        throw new UnsupportedOperationException( "Directly writing entity key ids is not supported." );
    }

    @Override public UUID load( EntityKey key ) {
        UUID id = super.load( key );

        while ( id == null ) {
            id = UUID.randomUUID();
            super.store( key, id );
            id = entityKeyIdCache.getIfPresent( key );
        }
        //Clean up the cache
        entityKeyIdCache.invalidate( key );
        return id;
    }

    @Override protected void handleStoreSucceeded( EntityKey key, UUID value ) {
        entityKeyIdCache.put( key, value );
        logger.debug( "Successfully stored key {} and value {} in map {}.", key, value, getMapName() );
    }

    @Override public Map<EntityKey, UUID> loadAll( Collection<EntityKey> keys ) {
        return super.loadAll( keys );
    }

    @Override protected List<PostgresColumnDefinition> initKeyColumns() {
        return ImmutableList.of( ENTITY_SET_ID, SYNC_ID, ENTITY_ID );
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( PostgresColumn.ID );
    }

    @Override
    protected void handleStoreFailed( EntityKey key, UUID value ) {
        double sleepTime = rateLimiter.acquire();
        logger.info( "Retrying store with new UUID after {} second.", sleepTime );
        super.store( key, UUID.randomUUID() );
    }

    public int bind( PreparedStatement ps, EntityKey key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key.getEntitySetId() );
        ps.setObject( parameterIndex++, key.getEntityId() );
        return parameterIndex;
    }

    public void bind( PreparedStatement ps, EntityKey key, UUID value ) throws SQLException {
        bind( ps, key, 1 );
        ps.setObject( 4, value );
    }

    public UUID mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    public EntityKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.entityKey( rs );
    }
}
