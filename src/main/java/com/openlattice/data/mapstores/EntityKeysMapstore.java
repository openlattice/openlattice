

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

package com.openlattice.data.mapstores;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openlattice.data.EntityKey;
import com.openlattice.mapstores.TestDataFactory;
import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TimestampGenerator;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraPartitionKeyValueStore;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityKeysMapstore extends AbstractStructuredCassandraPartitionKeyValueStore<UUID, EntityKey> {
    private static final Logger logger = LoggerFactory.getLogger( EntityKeysMapstore.class );
    public static final TimestampGenerator tg = new AtomicMonotonicTimestampGenerator();

    public EntityKeysMapstore(
            String mapName,
            Session session,
            CassandraTableBuilder tableBuilder ) {
        super( mapName, session, tableBuilder );
    }

    @Override
    public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override
    public EntityKey generateTestValue() {
        return TestDataFactory.entityKey();
    }

    @Override
    protected BoundStatement bind( UUID key, BoundStatement bs ) {
        return bs.setUUID( CommonColumns.ID.cql(), key );
    }

    @Override
    protected BoundStatement bind( UUID key, EntityKey value, BoundStatement bs ) {
        return bindStoreQuery( key, value, bs );
    }

    @Override public void store( UUID key, EntityKey value ) {
        throw new UnsupportedOperationException( "Entity keys mapstore is over a materialized view." );
    }

    @Override public void storeAll( Map<UUID, EntityKey> map ) {
        throw new UnsupportedOperationException( "Entity keys mapstore is over a materialized view." );
    }

    @Override
    protected UUID mapKey( Row rs ) {
        return RowAdapters.id( rs );
    }

    @Override public Iterable<UUID> loadAllKeys() {
        return null;
    }

    @Override
    protected EntityKey mapValue( ResultSet rs ) {
        Row r = rs.one();
        if ( r == null ) {
            return null;
        } else {
            EntityKey value = RowAdapters.entityKey( r );
            if( !rs.isExhausted() ){
                logger.error( "UUID {} corresponds to multiple entity keys.", RowAdapters.id( r ) );
            }
            return value;
        }
    }
    
    public static BoundStatement bindStoreQuery( UUID key, EntityKey value, BoundStatement bs ) {
        return bs
                .setUUID( CommonColumns.ID.cql(), key )
                .set( CommonColumns.ENTITY_KEY.cql(), value, EntityKey.class )
                .set( CommonColumns.TIMESTAMP.cql(), tg.next(), Long.class );
    }

//    @Override public MapStoreConfig getMapStoreConfig() {
//        return super.getMapStoreConfig().setInitialLoadMode( MapStoreConfig.InitialLoadMode.LAZY );
//    }
}
