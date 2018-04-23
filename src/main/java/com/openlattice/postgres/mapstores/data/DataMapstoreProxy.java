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

import com.dataloom.streams.StreamUtil;
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
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.PostgresTableManager;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataMapstoreProxy implements TestableSelfRegisteringMapStore<EntityDataKey, EntityDataValue> {
    public static final  String VERSION           = "version";
    public static final  String LAST_WRITE        = "lastWrite";
    public static final  String LAST_INDEX        = "lastIndex";
    public static final  String KEY_ENTITY_SET_ID = "__key#entitySetId";
    public static final  String KEY_ENTITY_KEY_ID = "__key#entityKeyId";
    private static final Logger logger            = LoggerFactory.getLogger( DataMapstoreProxy.class );
    private final Map<UUID, EntityDataMapstore>   entitySetMapstores; //Entity Set ID -> Mapstore for Entity Set Table
    private final Map<UUID, PropertyDataMapstore> propertyDataMapstores;

    private final HikariDataSource             hds;
    private final PostgresTableManager         pgTableMgr;
    private final MapStore<UUID, PropertyType> propertyTypes;
    private final MapStore<UUID, EntitySet>    entitySets;
    private final MapStore<UUID, EntityType>   entityTypes;

    public DataMapstoreProxy(
            PostgresTableManager pgTableMgr,
            HikariDataSource hds,
            MapStore<UUID, PropertyType> propertyTypes,
            MapStore<UUID, EntitySet> entitySets,
            MapStore<UUID, EntityType> entityTypes ) {
        this.entitySetMapstores = new HashMap<>();
        this.propertyDataMapstores = new HashMap<>();
        this.hds = hds;
        this.propertyTypes = propertyTypes;
        this.entitySets = entitySets;
        this.entityTypes = entityTypes;
        this.pgTableMgr = pgTableMgr;
    }

    public EntityDataMapstore getMapstore( UUID entitySetId ) {
        return entitySetMapstores.computeIfAbsent( entitySetId, this::newEntitySetMapStore );
    }

    public PropertyDataMapstore getPropertyMapstore( UUID propertyTypeId ) {
        return propertyDataMapstores
                .computeIfAbsent( propertyTypeId, ptId -> newPropertyDataMapstore( ptId ) );
    }

    protected EntityDataMapstore newEntitySetMapStore( UUID entitySetId ) {
        //        logger.info( "Starting table creation for entity set: ", es.getName() );
        //        EntityType et = etm.load( es.getEntityTypeId() );
        //        final Collection<PropertyType> pTypes = propertyTypes.loadAll( entityTypes.getProperties() ).values();
        //        try {
        //            logger.info( "Deleting entity set tables for entity set {}.", entitySets.getName() );
        //            pgEdmManager.deleteEntitySet( entitySets, propertyTypes );
        //            logger.info( "Creating entity set tables for entity set {}.", entitySets.getName() );
        //            pgEdmManager.createEntitySet( entitySets, propertyTypes );
        //        } catch ( SQLException e ) {
        //            logger.error( "Failed to create tables for entity set {}.", es, e );
        //        }
        //        logger.info( "Finished with table creation for entity set: {}", es.getName() );
        PostgresTableDefinition table = DataTables.buildEntitySetTableDefinition( entitySetId );
        try {
            synchronized ( pgTableMgr ) {
                pgTableMgr.registerTables( table );
            }
        } catch ( SQLException e ) {
            logger.error( "Unable to register entity set table {}", table, e );
        }
        return new EntityDataMapstore( hds, table );
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
                .setInitialLoadMode( MapStoreConfig.InitialLoadMode.EAGER )
                .setImplementation( this )
                .setEnabled( true )
                .setWriteDelaySeconds( 5 );
    }

    @Override
    public MapConfig getMapConfig() {
        return new MapConfig( getMapName() )
                .setMapStoreConfig( getMapStoreConfig() )
                .addMapIndexConfig( new MapIndexConfig( KEY_ENTITY_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( KEY_ENTITY_KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( VERSION, true ) )
                .addMapIndexConfig( new MapIndexConfig( LAST_WRITE, true ) )
                .addMapIndexConfig( new MapIndexConfig( LAST_INDEX, true ) );
    }

    @Override public void store( EntityDataKey key, EntityDataValue value ) {
        final UUID entityKeyId = key.getEntityKeyId();
        final EntityDataMapstore edms = getMapstore( key.getEntitySetId() );

        //Store the metadata
        edms.store( entityKeyId, value.getMetadata() );

        //Store the property values
        final Map<UUID, Map<Object, PropertyMetadata>> properties = value.getProperties();
        for ( Entry<UUID, Map<Object, PropertyMetadata>> propertyEntry : properties.entrySet() ) {
            final UUID propertyTypeId = propertyEntry.getKey();
            final PropertyDataMapstore propertyDataMapstore = getPropertyMapstore( propertyTypeId );
            propertyDataMapstore.store( key, propertyEntry.getValue() );
        }

    }

    @Override public void storeAll( Map<EntityDataKey, EntityDataValue> map ) {
        for ( Entry<EntityDataKey, EntityDataValue> e : map.entrySet() ) {
            store( e.getKey(), e.getValue() );
        }
    }

    @Override public void delete( EntityDataKey key ) {
        EntityDataMapstore edms = getMapstore( key.getEntitySetId() );
        edms.delete( key.getEntityKeyId() );
    }

    @Override public void deleteAll( Collection<EntityDataKey> keys ) {
        for ( EntityDataKey key : keys ) {
            delete( key );
        }
    }

    @Override public EntityDataValue load( EntityDataKey key ) {
        final UUID entitySetId = key.getEntitySetId();
        final UUID entityKeyId = key.getEntityKeyId();
        final EntitySet es = entitySets.load( entitySetId );

        //If entity set is not found return immediately.
        if ( es == null ) {
            return null;
        }

        final EntityType entityType = entityTypes.load( es.getEntityTypeId() );

        if ( entityType == null ) {
            return null;
        }

        final Set<UUID> propertyTypes = entityType.getProperties();

        final EntityDataMapstore edms = getMapstore( entitySetId );
        final EntityDataMetadata maybeMetadata = edms.load( entityKeyId );
        final EntityDataMetadata metadata =
                ( maybeMetadata == null
                        ? EntityDataMetadata.newEntityDataMetadata( OffsetDateTime.now() )
                        : maybeMetadata );
        final Map<UUID, Map<Object, PropertyMetadata>> properties = new HashMap<>();
        for ( UUID propertyTypeId : propertyTypes ) {
            final PropertyDataMapstore propertyDataMapstore = propertyDataMapstores
                    .computeIfAbsent( propertyTypeId, ptId -> newPropertyDataMapstore( ptId ) );
            final Map<Object, PropertyMetadata> propertiesOfType = propertyDataMapstore.load( key );
            properties.put( propertyTypeId, propertiesOfType == null ? new HashMap<>() : propertiesOfType );
        }

        return new EntityDataValue( metadata, properties );
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
        return () -> StreamUtil.stream( entitySets.loadAllKeys() )
                .flatMap( entitySetId -> StreamUtil
                        .stream( getMapstore( entitySetId ).loadAllKeys() )
                        .map( entityKeyId -> new EntityDataKey( entitySetId, entityKeyId ) ) ).iterator();

    }

    protected PropertyDataMapstore newPropertyDataMapstore( UUID propertyTypeId ) {
        PropertyType propertyType = propertyTypes.load( propertyTypeId );
        PostgresTableDefinition table = DataTables.buildPropertyTableDefinition( propertyType );
        try {
            synchronized ( pgTableMgr ) {
                pgTableMgr.registerTables( table );
            }
        } catch ( SQLException e ) {
            logger.error( "Unable to register property type table {}.", table, e );
        }
        ;
        return new PropertyDataMapstore( DataTables.value( propertyType ), table, hds );
    }
}
