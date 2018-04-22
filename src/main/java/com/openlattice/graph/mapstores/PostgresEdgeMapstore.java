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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdgeMapstore extends AbstractBasePostgresMapstore<EdgeKey, Edge> {
    public static final  String EDGE_SET_ID       = "edgeSetId";
    public static final  String SRC_ENTITY_KEY_ID = "srcEntityKeyId";
    public static final  String DST_ENTITY_KEY_ID = "dstEntityKeyId";
    public static final  String SRC_SET_ID        = "srcSetId";
    public static final  String DST_SET_ID        = "dstSetId";
    private static final Logger logger            = LoggerFactory.getLogger( PostgresEdgeMapstore.class );

    public PostgresEdgeMapstore( HikariDataSource hds ) throws SQLException {
        super( HazelcastMap.EDGES.name(), PostgresTable.EDGES, hds );
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

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig()
                .setImplementation( this )
                .setInitialLoadMode( InitialLoadMode.EAGER )
                .setEnabled( true )
                .setWriteDelaySeconds( 5 );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .setMapStoreConfig( getMapStoreConfig() )
                .setInMemoryFormat( InMemoryFormat.OBJECT )
                .addMapIndexConfig( new MapIndexConfig( SRC_ENTITY_KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( DST_ENTITY_KEY_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( DST_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( "dstSyncId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "srcTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( SRC_SET_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( "srcSyncId", false ) )
                .addMapIndexConfig( new MapIndexConfig( "edgeTypeId", false ) )
                .addMapIndexConfig( new MapIndexConfig( EDGE_SET_ID, false ) );
    }

    public int bind( PreparedStatement ps, EdgeKey key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key.getSrcEntityKeyId() );
        ps.setObject( parameterIndex++, key.getDstTypeId() );
        ps.setObject( parameterIndex++, key.getEdgeTypeId() );
        ps.setObject( parameterIndex++, key.getDstEntityKeyId() );
        ps.setObject( parameterIndex++, key.getEdgeEntityKeyId() );
        return parameterIndex;
    }

    public void bind( PreparedStatement ps, EdgeKey key, Edge value ) throws SQLException {
        //This mapstore is exception to the rule-- order of key and value bindings is different.
        ps.setObject( 1, value.getSrcEntityKeyId() );
        ps.setObject( 2, value.getDstTypeId() );
        ps.setObject( 3, value.getEdgeTypeId() );
        ps.setObject( 4, value.getDstEntityKeyId() );
        ps.setObject( 5, value.getEdgeEntityKeyId() );
        ps.setObject( 6, value.getSrcTypeId() );
        ps.setObject( 7, value.getSrcSetId() );
        ps.setObject( 8, value.getSrcSyncId() );
        ps.setObject( 9, value.getDstSetId() );
        ps.setObject( 10, value.getDstSyncId() );
        ps.setObject( 11, value.getEdgeSetId() );

        //Update
        ps.setObject( 12, value.getSrcTypeId() );
        ps.setObject( 13, value.getSrcSetId() );
        ps.setObject( 14, value.getSrcSyncId() );
        ps.setObject( 15, value.getDstSetId() );
        ps.setObject( 16, value.getDstSyncId() );
        ps.setObject( 17, value.getEdgeSetId() );
    }

    public Edge mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.loomEdge( rs );
    }

    public EdgeKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.edgeKey( rs );
    }

}
