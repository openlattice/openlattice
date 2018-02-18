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

package com.openlattice.graph.mapstores;

import com.openlattice.graph.edge.Edge;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.openlattice.conductor.codecs.odata.Table;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

public class EdgeMapstore extends AbstractStructuredCassandraMapstore<EdgeKey, Edge> {

    public static final String SRC_ENTITY_KEY_ID = "srcEntityKeyId";

    public EdgeMapstore( Session session ) {
        super( HazelcastMap.EDGES.name(), session, Table.EDGES.getBuilder() );
    }

    @Override public EdgeKey generateTestKey() {
        return new EdgeKey( new UUID( 0, 0 ),
                new UUID( 0, 1 ),
                new UUID( 0, 2 ),
                new UUID( 0, 3 ),
                new UUID( 0, 4 ) );
    }

    @Override public Edge generateTestValue() {
        return new Edge( generateTestKey(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID() );
    }

    @Override
    protected BoundStatement bind( EdgeKey key, BoundStatement bs ) {
        return bs.bind()
                .setUUID( CommonColumns.SRC_ENTITY_KEY_ID.cql(), key.getSrcEntityKeyId() )
                .setUUID( CommonColumns.DST_TYPE_ID.cql(), key.getDstTypeId() )
                .setUUID( CommonColumns.EDGE_TYPE_ID.cql(), key.getEdgeTypeId() )
                .setUUID( CommonColumns.DST_ENTITY_KEY_ID.cql(), key.getDstEntityKeyId() )
                .setUUID( CommonColumns.EDGE_ENTITY_KEY_ID.cql(), key.getEdgeEntityKeyId() );

    }

    @Override
    protected BoundStatement bind( EdgeKey key, Edge value, BoundStatement bs ) {
        return bind( key, bs )
                .setUUID( CommonColumns.SRC_TYPE_ID.cql(), value.getSrcTypeId() )
                .setUUID( CommonColumns.SRC_ENTITY_SET_ID.cql(), value.getSrcSetId() )
                .setUUID( CommonColumns.SRC_SYNC_ID.cql(), value.getSrcSyncId() )
                .setUUID( CommonColumns.DST_ENTITY_SET_ID.cql(), value.getDstSetId() )
                .setUUID( CommonColumns.DST_SYNC_ID.cql(), value.getDstSyncId() )
                .setUUID( CommonColumns.EDGE_ENTITY_SET_ID.cql(), value.getEdgeSetId() );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig().setWriteDelaySeconds( 5 );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( SRC_ENTITY_KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstEntityKeyId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstSetId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstSyncId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "srcTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "srcSetId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "srcSyncId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "edgeTypeId", false ) );
    }

    @Override public Iterable<EdgeKey> loadAllKeys() {
        return super.loadAllKeys();
    }

    @Override public void store( EdgeKey key, Edge value ) {
        super.store( key, value );
    }

    @Override protected EdgeKey mapKey( Row row ) {
        return RowAdapters.edgeKey( row );
    }

    @Override protected Edge mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null : RowAdapters.loomEdge( row );
    }
}
