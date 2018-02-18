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
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.data.hazelcast.DataKey;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.edm.type.PropertyType;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataMapstore
        extends AbstractStructuredCassandraMapstore<DataKey, ByteBuffer> {
    public static final HashFunction hf                   = Hashing.murmur3_128();
    public static final String       KEY_ID               = "__key#id";
    public static final String       KEY_ENTITY_SET_ID    = "__key#entitySetId";
    public static final String       KEY_SYNC_ID          = "__key#syncId";
    public static final String       KEY_PROPERTY_TYPE_ID = "__key#propertyTypeId";

    public DataMapstore(
            String mapName,
            CassandraTableBuilder tableBuilder,
            Session session,
            SelfRegisteringMapStore<UUID, PropertyType> ptMapStore,
            ObjectMapper mapper ) {
        super( mapName, session, tableBuilder );
    }

    @Override
    public DataKey generateTestKey() {
        return null;
    }

    @Override
    public ByteBuffer generateTestValue() {
        return null;
    }

    @Override
    protected BoundStatement bind( DataKey key, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.ID.cql(), key.getId() )
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), key.getEntitySetId() )
                .setUUID( CommonColumns.SYNCID.cql(), key.getSyncId() )
                .setString( CommonColumns.ENTITYID.cql(), key.getEntityId() )
                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), key.getPropertyTypeId() )
                .setBytes( CommonColumns.PROPERTY_VALUE.cql(), ByteBuffer.wrap( key.getHash() ) );
    }

    @Override
    protected BoundStatement bind(
            DataKey key, ByteBuffer value, BoundStatement bs ) {
        return bind( key, bs )
                .setBytes( CommonColumns.PROPERTY_BUFFER.cql(), value );
    }

    @Override public ByteBuffer load( DataKey key ) {
        return super.load( key );
    }

    @Override public Map<DataKey, ByteBuffer> loadAll( Collection<DataKey> keys ) {
        return super.loadAll( keys );
    }

    //    private BoundStatement bindProperty( Entry<UUID, byte[]> property ) {
    //        return getStoreQuery().bind()
    //                .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), property.getAclKey() )
    //                .setBytes( CommonColumns.PROPERTY_VALUE.cql(),
    //                        ByteBuffer.wrap( hf.hashBytes( property.getValue() ).asBytes() ) )
    //                .setBytes( CommonColumns.PROPERTY_BUFFER.cql(), ByteBuffer.wrap( property.getValue() ) );
    //
    //    }

    @Override
    public Iterable<DataKey> loadAllKeys() {
        return StreamUtil.stream( session
                .execute( currentSyncs() ) )
                .parallel()
                .map( cs ->
                        session.executeAsync( tableBuilder.buildLoadAllPrimaryKeysQuery().allowFiltering()
                                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(),
                                        cs.getUUID( CommonColumns.ENTITY_SET_ID.cql() ) ) )
                                .and( QueryBuilder.eq( CommonColumns.SYNCID.cql(),
                                        cs.getUUID( CommonColumns.CURRENT_SYNC_ID.cql() ) ) ) ) )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( DataKey::fromRow )::iterator;
    }

    @Override
    protected DataKey mapKey( Row row ) {
        return DataKey.fromRow( row );
    }

    @Override
    protected ByteBuffer mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : row.getBytes( CommonColumns.PROPERTY_BUFFER.cql() );

        //        final SetMultimap<UUID, byte[]> m = HashMultimap.create();
        //        EntityKey ek = null;
        //        for ( Row row : rs ) {
        //            if ( ek == null ) {
        //                ek = RowAdapters.entityKeyFromData( row );
        //            }
        //            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
        //            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
        //            ByteBuffer property = row.getBytes( CommonColumns.PROPERTY_BUFFER.cql() );
        //            m.put( propertyTypeId, property.array() );
        //        }
        //        if ( ek == null ) {
        //            return null;
        //        }
        //        return new EntityBytes( ek, m );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig().setWriteDelaySeconds( 5 );//.setInitialLoadMode( InitialLoadMode.EAGER );
    }

    @Override
    public MapConfig getMapConfig() {
        return super.getMapConfig()
                .setBackupCount( 1 )
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( KEY_ENTITY_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( KEY_SYNC_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( KEY_PROPERTY_TYPE_ID, false ) );
    }

    public static Select currentSyncs() {
        return QueryBuilder.select( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.CURRENT_SYNC_ID.cql() )
                .distinct().from( Table.SYNC_IDS.getKeyspace(), Table.SYNC_IDS.getName() );

    }

}
