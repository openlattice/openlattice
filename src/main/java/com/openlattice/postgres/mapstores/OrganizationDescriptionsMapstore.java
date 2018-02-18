package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.DESCRIPTION;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

public class OrganizationDescriptionsMapstore extends AbstractBasePostgresMapstore<UUID, String> {

    public OrganizationDescriptionsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ORGANIZATIONS_DESCRIPTIONS.name(), ORGANIZATIONS, hds );
    }

    @Override public List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( DESCRIPTION );
    }

    @Override public void bind( PreparedStatement ps, UUID key, String value ) throws SQLException {
        bind( ps, key, 1 );
        ps.setString( 2, value );

        // UPDATE
        ps.setString( 3, value );
    }

    @Override public int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override public String mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.description( rs );
    }

    @Override public UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    public Map<UUID, String> loadAll( Collection<UUID> keys ) {
        Map<UUID, String> result = Maps.newConcurrentMap();
        keys.parallelStream().forEach( id -> {
            String description = load( id );
            if ( description != null ) { result.put( id, description ); }
        } );
        return result;
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, DESCRIPTION );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public String generateTestValue() {
        return RandomStringUtils.random( 10 );
    }
}
