package com.openlattice.postgres.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.NULLABLE_TITLE;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

public class OrganizationTitlesMapstore extends AbstractBasePostgresMapstore<UUID, String> {

    public OrganizationTitlesMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ORGANIZATIONS_TITLES.name(), ORGANIZATIONS, hds );
    }


    @Override public List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( NULLABLE_TITLE );
    }

    @Override public void bind( PreparedStatement ps, UUID key, String value ) throws SQLException {
        bind( ps, key , 1 );
        ps.setString( 2, value );

        // UPDATE
        ps.setString( 3, value );
    }

    @Override public int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override public String mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.nullableTitle( rs );
    }

    @Override public UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, NULLABLE_TITLE );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public String generateTestValue() {
        return RandomStringUtils.random( 10 );
    }
}
