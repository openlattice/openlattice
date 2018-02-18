package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.EDM_VERSION;
import static com.openlattice.postgres.PostgresColumn.EDM_VERSION_NAME;
import static com.openlattice.postgres.PostgresTable.EDM_VERSIONS;

import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

public class EdmVersionsMapstore extends AbstractBasePostgresMapstore<String, UUID> {
    private final HikariDataSource hds;

    public EdmVersionsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.EDM_VERSIONS.name(), EDM_VERSIONS, hds );
        this.hds = hds;
    }

    @Override protected String buildSelectByKeyQuery() {
        return "SELECT * FROM ".concat( EDM_VERSIONS.getName() ).concat( " WHERE " )
                .concat( EDM_VERSION_NAME.getName() ).concat( " = ? ORDER BY " ).concat( EDM_VERSION.getName() )
                .concat( " DESC LIMIT 1;" );
    }

    @Override protected Optional<String> buildOnConflictQuery() {
        return Optional.empty();
    }

    @Override protected void bind( PreparedStatement ps, String key, UUID value ) throws SQLException {
        bind( ps, key, 1 );
        ps.setObject( 2, value );
    }

    @Override protected int bind( PreparedStatement ps, String key, int parameterIndex ) throws SQLException {
        ps.setString( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected UUID mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.edmVersion( rs );
    }

    @Override protected String mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.edmVersionName( rs );
    }

    @Override public String generateTestKey() {
        return RandomStringUtils.random( 10 );
    }

    @Override public UUID generateTestValue() {
        return UUID.randomUUID();
    }
}
