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

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.Maps;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.postgres.CountdownConnectionCloser;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresDataMapstore implements TestableSelfRegisteringMapStore<DataKey, ByteBuffer> {
    private static final Logger logger        = LoggerFactory.getLogger( PostgresDataMapstore.class );
    //TODO: We no longer need to store hash with postgres unique, especially once we move to SetMultimap datastructure
    private static final String CREATE_TABLE  = "CREATE TABLE IF NOT EXISTS data(\n"
            + "id UUID,\n"
            + "entity_set_id UUID, syncid UUID, entityid text, property_type_id UUID, property_value bytea, property_buffer bytea, PRIMARY KEY (id, entity_set_id, syncid, entityid, property_type_id, property_value ) );";
    private static final String INSERT_ROW    = "INSERT INTO data (id,entity_set_id, syncid, entityid, property_type_id, property_value, property_buffer ) values ( ?,?,?,?,?,?,? ) on conflict do nothing;";
    private static final String SELECT_ROW    = "SELECT * from data where id = ? and entity_set_id = ? and syncid = ? and entityid = ? and property_type_id = ? and property_value = ?";
    private static final String DELETE_ROW    = "DELETE from data where id = ? and entity_set_id = ? and syncid = ? and entityid = ? and property_type_id = ? and property_value = ?";
    private static final String LOAD_ALL_KEYS = "SELECT id, entity_set_id, syncid, entityid, property_type_id, property_value FROM data where entity_set_id = ? and syncid = ?";
    private static final String CURRENT_SYNCS = "SELECT DISTINCT entity_set_id, current_sync_id FROM sync_ids";
    private final HikariDataSource hds;
    private final String           mapName;

    public PostgresDataMapstore(
            String mapName,
            HikariDataSource hds ) throws SQLException {
        this.hds = hds;
        this.mapName = mapName;
        try ( Connection connection = hds.getConnection(); Statement statement = connection.createStatement() ) {
            statement.execute( CREATE_TABLE );
            connection.close();
            logger.info( "Initialized Postgres Data Mapstore" );
        } catch ( SQLException e ) {
            logger.error( "Unable to initialize Postgres Data Mapstore", e );
        }
    }

    @Override public void store( DataKey key, ByteBuffer value ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement insertRow = connection.prepareStatement( INSERT_ROW );
            bind( insertRow, key, value );
            insertRow.executeUpdate();
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store for key {}.", key, e );
        }
    }

    @Override public void storeAll( Map<DataKey, ByteBuffer> map ) {
        DataKey key = null;
        try ( Connection connection = hds.getConnection();
                PreparedStatement insertRow = connection.prepareStatement( INSERT_ROW ) ) {
            connection.setAutoCommit( false );
            for ( Entry<DataKey, ByteBuffer> entry : map.entrySet() ) {
                key = entry.getKey();
                bind( insertRow, key, entry.getValue() );
                try {
                    insertRow.executeUpdate();
                } catch ( SQLException e ) {
                    connection.commit();
                    logger.error( "Unable to store row {} -> {}",
                            key,
                            Base64.getEncoder().encodeToString( entry.getValue().array() ), e );
                }
            }
            connection.commit();
            connection.setAutoCommit( true );
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store all for key {}", key, e );
        }
    }

    @Override public void delete( DataKey key ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement deleteRow = connection.prepareStatement( DELETE_ROW );
            bind( deleteRow, key );
            deleteRow.executeUpdate();
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during delete for key {}.", key, e );
        }
    }

    @Override public void deleteAll( Collection<DataKey> keys ) {
        DataKey key = null;
        try ( Connection connection = hds.getConnection();
                PreparedStatement deleteRow = connection.prepareStatement( DELETE_ROW ) ) {
            connection.setAutoCommit( false );
            for ( DataKey entry : keys ) {
                key = entry;
                bind( deleteRow, key );
                deleteRow.executeUpdate();
            }
            connection.commit();
            connection.setAutoCommit( true );
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during delete all for key {}", key, e );
        }
    }

    @Override
    public ByteBuffer load( DataKey key ) {
        ByteBuffer val = null;
        try ( Connection connection = hds.getConnection();
                PreparedStatement selectRow = connection.prepareStatement( SELECT_ROW ) ) {
            bind( selectRow, key );
            ResultSet rs = selectRow.executeQuery();
            if ( rs.next() ) {
                val = mapToValue( rs );
                logger.debug( "LOADED: {}", val.array() );
            }
            rs.close();
            connection.close();
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during select for key {}.", key, e );
        }
        return val;
    }

    @Override
    public Map<DataKey, ByteBuffer> loadAll( Collection<DataKey> keys ) {
        return keys.parallelStream().collect( Collectors.toConcurrentMap( Function.identity(), this::load ) );
    }

    @Override public Iterable<DataKey> loadAllKeys() {
        Stream<DataKey> keys;
        Map<UUID, UUID> entitySetsToLoad = currentSyncs();

        logger.info( "Loading {} entity sets:\n{}", entitySetsToLoad.size(), entitySetsToLoad );

        try {
            final Connection connection = hds.getConnection();
            CountdownConnectionCloser closer = new CountdownConnectionCloser( connection, entitySetsToLoad.size() );
            return entitySetsToLoad
                    .entrySet()
                    .parallelStream()
                    .map( entry ->
                            new DataKeyIterator(
                                    connection,
                                    entry.getKey(),
                                    entry.getValue(),
                                    closer ) )
                    .flatMap( dki -> StreamUtil.stream( () -> dki ) )
                    .peek( key -> logger.debug( "Key to load: {}", key ) )
                    ::iterator;
        } catch ( SQLException e ) {
            logger.error( "Unable to acquire connection load all keys" );
            return null;
        }
    }

    @Override public String getMapName() {
        return mapName;
    }

    @Override public String getTable() {
        throw new NotImplementedException( "This shouldn't be invoked. Blame MTR." );
    }

    @Override public DataKey generateTestKey() {
        return new DataKey( UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                RandomStringUtils.random( 5 ),
                UUID.randomUUID(),
                RandomUtils.nextBytes( 16 ) );
    }

    @Override public ByteBuffer generateTestValue() {
        return ByteBuffer.wrap( RandomUtils.nextBytes( 32 ) );
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig()
                .setImplementation( this )
                .setEnabled( true )
                .setInitialLoadMode( InitialLoadMode.EAGER )
                .setWriteDelaySeconds( 5 );
    }

    @Override
    public MapConfig getMapConfig() {
        return new MapConfig( mapName )
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .setBackupCount( 1 )
                .setMapStoreConfig( getMapStoreConfig() )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_ENTITY_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_SYNC_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DataMapstore.KEY_PROPERTY_TYPE_ID, false ) );
    }

    public Map<UUID, UUID> currentSyncs() {
        try ( Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( CURRENT_SYNCS ) ) {
            Map<UUID, UUID> entitySetAndSyncIds = Maps.newHashMap();
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                UUID entitySetId = ResultSetAdapters.entitySetId( rs );
                UUID syncId = ResultSetAdapters.currentSyncId( rs );
                entitySetAndSyncIds.put( entitySetId, syncId );
            }
            connection.close();
            return entitySetAndSyncIds;
        } catch ( SQLException e ) {
            logger.error( "Unable to load current syncs." );
            return Maps.newHashMap();
        }
    }

    public static ByteBuffer mapToValue( ResultSet rs ) {
        try {
            return ByteBuffer.wrap( rs.getBytes( "property_buffer" ) );
        } catch ( SQLException e ) {
            logger.error( "Unable to map to value.", e );
            return null;
        }
    }

    public static DataKey mapToKey( ResultSet rs ) {
        try {
            UUID id = (UUID) rs.getObject( "id" );
            UUID entitySetId = (UUID) rs.getObject( "entity_set_id" );
            UUID syncId = (UUID) rs.getObject( "syncid" );
            String entityId = rs.getString( "entityId" );
            UUID propertyTypeId = (UUID) rs.getObject( "property_type_id" );
            byte[] hash = rs.getBytes( "property_value" );
            return new DataKey( id, entitySetId, syncId, entityId, propertyTypeId, hash );
        } catch ( SQLException e ) {
            logger.error( "Unable to map data key.", e );
            return null;
        }
    }

    public static void bind( PreparedStatement ps, DataKey key ) throws SQLException {
        ps.setObject( 1, key.getId() );
        ps.setObject( 2, key.getEntitySetId() );
        ps.setObject( 3, key.getSyncId() );
        ps.setString( 4, key.getEntityId() );
        ps.setObject( 5, key.getPropertyTypeId() );
        ps.setBytes( 6, key.getHash() );
    }

    public static void bind( PreparedStatement ps, DataKey key, ByteBuffer value ) throws SQLException {
        bind( ps, key );
        ps.setBytes( 7, value.array() );
    }

    static class DataKeyIterator implements Iterator<DataKey> {
        private final ResultSet                 rs;
        private final CountdownConnectionCloser closer;
        private       boolean                   next;

        public DataKeyIterator(
                Connection connection,
                UUID entitySetId,
                UUID syncId,
                CountdownConnectionCloser closer ) {
            this.closer = closer;
            try {
                PreparedStatement ps = connection.prepareStatement( LOAD_ALL_KEYS );
                ps.setObject( 1, entitySetId );
                ps.setObject( 2, syncId );
                rs = ps.executeQuery();
                next = rs.next();
                if ( !next ) {
                    closer.countDown();
                }
            } catch ( SQLException e ) {
                logger.error( "Unable to execute sql for load all keys for data map store" );
                throw new IllegalStateException( "Unable to execute sql statement", e );
            }
        }

        @Override public boolean hasNext() {
            return next;
        }

        @Override public DataKey next() {
            DataKey key;
            if ( next ) {
                key = mapToKey( rs );
            } else {
                throw new NoSuchElementException( "No more elements available in iterator" );

            }
            try {
                next = rs.next();
            } catch ( SQLException e ) {
                logger.error( "Unable to retrieve next result from result set." );
                next = false;
            }

            if ( !next ) {
                closer.countDown();
            }

            return key;
        }
    }
}
