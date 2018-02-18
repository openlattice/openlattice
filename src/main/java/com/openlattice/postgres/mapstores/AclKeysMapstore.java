package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresTable.ACL_KEYS;

import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.apache.commons.lang.RandomStringUtils;

public class AclKeysMapstore extends AbstractBasePostgresMapstore<String, UUID> {

    public AclKeysMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ACL_KEYS.name(), ACL_KEYS, hds );
    }

    @Override protected void bind( PreparedStatement ps, String key, UUID value ) throws SQLException {
        bind( ps, key, 1 );
        ps.setObject( 2, value );

        // UPDATE
        ps.setObject( 3, value );
    }

    @Override protected int bind( PreparedStatement ps, String key, int parameterIndex ) throws SQLException {
        ps.setString( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.securableObjectId( rs );
    }

    @Override protected String mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.name( rs );
    }

    @Override public String generateTestKey() {
        return RandomStringUtils.random( 5 );
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
