package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.NEW_VERTEX_ID;
import static com.openlattice.postgres.PostgresTable.VERTEX_IDS_AFTER_LINKING;

import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class VertexIdsAfterLinkingMapstore extends AbstractBasePostgresMapstore<LinkingVertexKey, UUID> {

    public VertexIdsAfterLinkingMapstore( HikariDataSource hds ) {
        super( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name(), VERTEX_IDS_AFTER_LINKING, hds );
    }

    @Override protected void bind( PreparedStatement ps, LinkingVertexKey key, UUID value ) throws SQLException {
        bind( ps, key, 1 );
        ps.setObject( 3, value );

        // UPDATE
        ps.setObject( 4, value );
    }

    @Override protected int bind( PreparedStatement ps, LinkingVertexKey key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key.getGraphId() );
        ps.setObject( parameterIndex++, key.getVertexId() );
        return parameterIndex;
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return rs.getObject( NEW_VERTEX_ID.getName(), UUID.class );
    }

    @Override protected LinkingVertexKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.linkingVertexKey( rs );
    }

    @Override
    public Iterable<LinkingVertexKey> loadAllKeys() {
        //lazy loading
        return null;
    }

    @Override public LinkingVertexKey generateTestKey() {
        return new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
