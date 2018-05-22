/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.postgres.mapstores.data;

import static com.openlattice.postgres.DataTables.LAST_INDEX;
import static com.openlattice.postgres.DataTables.LAST_WRITE;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.VERSION;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.MapStore;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresDataMapstoreProxy implements TestableSelfRegisteringMapStore<EntityDataKey, EntityDataValue> {
    public static final  String VERSION_INDEX           = "version";
    public static final  String LAST_WRITE_INDEX        = "lastWrite";
    public static final  String LAST_INDEX_INDEX        = "lastIndex";
    public static final  String KEY_ENTITY_SET_ID_INDEX = "key#entitySetId";
    private static final Logger logger                  = LoggerFactory.getLogger( PostgresDataMapstoreProxy.class );

    private final Map<UUID, Map<UUID, PostgresTableDefinition>> propertyTables;

    private final Map<UUID, String>            entityInsertQueries;
    private final Map<UUID, Map<UUID, String>> propertyInsertQueries;

    private final Map<UUID, EntityDataMapstore>              entitySetMapstores; //Entity Set ID -> Mapstore for Entity Set Table
    private final Map<UUID, Map<UUID, PropertyDataMapstore>> propertyDataMapstores;

    private final HikariDataSource             hds;
    private final MapStore<UUID, PropertyType> propertyTypes;
    private final MapStore<UUID, EntitySet>    entitySets;
    private final MapStore<UUID, EntityType>   entityTypes;

    public PostgresDataMapstoreProxy(
            // Map<UUID, EntityDataMapstore> entitySetMapstores,
            //            Map<UUID, Map<UUID, PropertyDataMapstore>> propertyDataMapstores,
            HikariDataSource hds,
            MapStore<UUID, PropertyType> propertyTypes,
            MapStore<UUID, EntitySet> entitySets,
            MapStore<UUID, EntityType> entityTypes ) {
        this.entityInsertQueries = new MapMaker()
                .initialCapacity( 10000 )
                .concurrencyLevel( Runtime.getRuntime().availableProcessors() - 1 )
                .makeMap();
        this.propertyInsertQueries = new MapMaker()
                .initialCapacity( 10000 )
                .concurrencyLevel( Runtime.getRuntime().availableProcessors() - 1 )
                .makeMap();
        this.propertyTables = new MapMaker()
                .initialCapacity( 10000 )
                .concurrencyLevel( Runtime.getRuntime().availableProcessors() - 1 )
                .makeMap();

        this.entitySetMapstores = new HashMap<>();
        this.propertyDataMapstores = new HashMap<>();
        this.hds = hds;
        this.propertyTypes = propertyTypes;
        this.entitySets = entitySets;
        this.entityTypes = entityTypes;
    }

    public String getEntityInsertQuery( UUID entitySetId ) {
        return entityInsertQueries.computeIfAbsent( entitySetId,
                esId -> DataTables.buildEntitySetTableDefinition( entitySetId )
                        .insertQuery( ID, VERSION, LAST_WRITE, LAST_INDEX ) );

    }

    public PreparedStatement prepareEntityInsertQuery( Connection connection, UUID entitySetId ) throws SQLException {
        return connection.prepareStatement( getEntityInsertQuery( entitySetId ) );
    }

    public PreparedStatement preparePropertyInsertQuery(
            Connection connection,
            UUID entitySetId,
            UUID propertyTypeId ) throws SQLException {
        return connection.prepareStatement( getPropertyInsertQuery( entitySetId, propertyTypeId ) );
    }

    protected Optional<String> buildOnConflictQuery( PostgresTableDefinition ptd ) {
        List<PostgresColumnDefinition> keyColumns = ImmutableList.copyOf( ptd.getPrimaryKey() );
        List<PostgresColumnDefinition> valueColumns = getValueColumns( ptd );
        return Optional.of( ( " ON CONFLICT ("
                + keyColumns.stream()
                .map( PostgresColumnDefinition::getName )
                .collect( Collectors.joining( ", " ) )
                + ") DO "
                + ptd.updateQuery( keyColumns, valueColumns, false ) ) );
    }

    public static List<PostgresColumnDefinition> getValueColumns( PostgresTableDefinition ptd ) {
        return ImmutableList
                .copyOf( Sets.difference( ptd.getColumns(), ptd.getPrimaryKey() ) );
    }


    public String getPropertyInsertQuery( UUID entitySetId, UUID propertyTypeId ) {
        Map<UUID, String> queryMap = propertyInsertQueries
                .computeIfAbsent( entitySetId, esId -> Maps.newConcurrentMap() );
        PostgresTableDefinition propTable = DataTables
                .buildPropertyTableDefinition( propertyTypes.load( propertyTypeId ) );
        List<PostgresColumnDefinition> propTableCols = getValueColumns( propTable );
        //This will create an on conflict do update query.
        return queryMap.computeIfAbsent( propertyTypeId,
                ptId ->
                        propTable.insertQuery( buildOnConflictQuery(propTable), propTableCols) );
    }

    @Override public String getMapName() {
        return HazelcastMap.ENTITY_DATA.name();
    }

    @Override public String getTable() {
        return getMapName();
    }

    @Override public EntityDataKey generateTestKey() {
        return null;
    }

    @Override public EntityDataValue generateTestValue() {
        return null;
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig()
                .setImplementation( this )
                .setEnabled( true )
                .setWriteDelaySeconds( 5 );
    }

    @Override
    public MapConfig getMapConfig() {
        return new MapConfig( getMapName() )
                .setMapStoreConfig( getMapStoreConfig() )
                .addMapIndexConfig( new MapIndexConfig( KEY_ENTITY_SET_ID_INDEX, false ) )
                .addMapIndexConfig( new MapIndexConfig( VERSION_INDEX, true ) )
                .addMapIndexConfig( new MapIndexConfig( LAST_WRITE_INDEX, true ) )
                .addMapIndexConfig( new MapIndexConfig( LAST_INDEX_INDEX, true ) );
    }

    @Override public void store( EntityDataKey key, EntityDataValue value ) {
        final UUID entitySetId = key.getEntitySetId();
//        final UUID entityKeyId = key.getEntityTypeId();

        try ( Connection conn = hds.getConnection(); PreparedStatement ps = prepareEntityInsertQuery( conn, entitySetId ) ) {

            final Map<UUID, Map<Object, PropertyMetadata>> properties = value.getProperties();
            for ( Entry<UUID, Map<Object, PropertyMetadata>> propertyEntry : properties.entrySet() ) {
                final UUID propertyTypeId = propertyEntry.getKey();
                try ( PreparedStatement pps = preparePropertyInsertQuery( conn, entitySetId, propertyTypeId ) ) {

                }
            }
        } catch ( SQLException e ) {
            logger.error( "Error executing SQL during store all for key {} and value {} in data mapstore",
                    key,
                    value,
                    e );
        }

//        final EntityDataMapsto    re edms = getMapstore( key.getEntitySetId() );
//
//        //Store the metadata
//        edms.store( entityKeyId, value.getMetadata() );
//
//        //Store the property values
//        final Map<UUID, Map<Object, PropertyMetadata>> properties = value.getProperties();
//        for ( Entry<UUID, Map<Object, PropertyMetadata>> propertyEntry : properties.entrySet() ) {
//            final UUID propertyTypeId = propertyEntry.getKey();
//            final PropertyDataMapstore propertyDataMapstore = propertyDataMapstores
//                    .computeIfAbsent( entitySetId, esId -> new HashMap<>() )
//                    .computeIfAbsent( propertyTypeId, ptId -> newPropertyDataMapstore( entitySetId, ptId ) );
//            propertyDataMapstore.store( entityKeyId, propertyEntry.getValue() );
//        }

    }

    @Override public void storeAll( Map<EntityDataKey, EntityDataValue> map ) {
        for ( Entry<EntityDataKey, EntityDataValue> e : map.entrySet() ) {
            store( e.getKey(), e.getValue() );
        }
    }

    @Override public void delete( EntityDataKey key ) {
//        EntityDataMapstore edms = getMapstore( key.getEntitySetId() );
//        edms.delete( key.getEntityTypeId() );
    }

    @Override public void deleteAll( Collection<EntityDataKey> keys ) {
        for ( EntityDataKey key : keys ) {
            delete( key );
        }
    }

    @Override public EntityDataValue load( EntityDataKey key ) {
        final UUID entitySetId = key.getEntitySetId();
//        final UUID entityKeyId = key.getEntityTypeId();
        final EntitySet es = entitySets.load( entitySetId );

        //If entity set is not found return immediately.
        if ( es == null ) {
            return null;
        }

        final EntityType entityType = entityTypes.load( es.getEntityTypeId() );

        if ( entityType == null ) {
            return null;
        }

//        final Set<UUID> propertyTypes = entityType.getProperties();
//
//        final EntityDataMapstore edms = getMapstore( entitySetId );
//        final EntityDataMetadata metadata = edms.load( entityKeyId );
//        final Map<UUID, Map<Object, PropertyMetadata>> properties = new HashMap<>();
//        for ( UUID propertyTypeId : propertyTypes ) {
//            final PropertyDataMapstore propertyDataMapstore = propertyDataMapstores
//                    .computeIfAbsent( entitySetId, esId -> new HashMap<>() )
//                    .computeIfAbsent( propertyTypeId, ptId -> newPropertyDataMapstore( entitySetId, ptId ) );
//            final Map<Object, PropertyMetadata> propertiesOfType = propertyDataMapstore.load( entityKeyId );
//            properties.put( propertyTypeId, propertiesOfType );
//        }
//
//        return new EntityDataValue( metadata, properties );
    return null;
    }

    @Override public Map<EntityDataKey, EntityDataValue> loadAll( Collection<EntityDataKey> keys ) {
        Map<EntityDataKey, EntityDataValue> m = new HashMap<>();
        keys.forEach( k -> {
            EntityDataValue edv = load( k );
            if ( k != null ) {
                m.put( k, edv );
            }
        } );
        return m;
    }

    @Override public Iterable<EntityDataKey> loadAllKeys() {
//        return () -> StreamUtil.stream( entitySets.loadAllKeys() )
//                .flatMap( entitySetId -> StreamUtil
//                        .stream( getMapstore( entitySetId ).loadAllKeys() )
//                        .map( entityKeyId -> new EntityDataKey( entitySetId, entityKeyId ) ) ).iterator();
        return null;

    }

    protected PropertyDataMapstore newPropertyDataMapstore(  UUID propertyTypeId ) {
        PropertyType propertyType = propertyTypes.load( propertyTypeId );
        PostgresTableDefinition table = DataTables.buildPropertyTableDefinition( propertyType );
        return new PropertyDataMapstore( DataTables.value( propertyType ),table, hds );
    }
}
