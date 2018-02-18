package com.openlattice.postgres.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.NAME;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECTID;
import static com.openlattice.postgres.PostgresTable.NAMES;

public class NamesMapstore extends AbstractBasePostgresMapstore<UUID, String> {

    public NamesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.NAMES.name(), NAMES, hds );
    }


    @Override protected void bind( PreparedStatement ps, UUID key, String value ) throws SQLException {
        bind( ps, key ,1 );
        ps.setString( 2, value );

        // UPDATE
        ps.setString( 3, value );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected String mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.name( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.securableObjectId( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public String generateTestValue() {
        return "testValue";
    }
}
