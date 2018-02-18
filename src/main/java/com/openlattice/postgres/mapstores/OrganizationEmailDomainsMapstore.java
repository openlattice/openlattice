package com.openlattice.postgres.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.RandomStringUtils;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.ALLOWED_EMAIL_DOMAINS;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

public class OrganizationEmailDomainsMapstore extends AbstractBasePostgresMapstore<UUID, DelegatedStringSet> {

    public OrganizationEmailDomainsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ALLOWED_EMAIL_DOMAINS.name(), ORGANIZATIONS, hds );
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( ALLOWED_EMAIL_DOMAINS );
    }

    @Override protected void bind(
            PreparedStatement ps, UUID key, DelegatedStringSet value ) throws SQLException {
        bind( ps, key, 1 );

        Array valueArr = PostgresArrays.createTextArray( ps.getConnection(), value.stream() );
        ps.setObject( 2, valueArr );

        // UPDATE
        ps.setObject( 3, valueArr );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected DelegatedStringSet mapToValue( ResultSet rs ) throws SQLException {
        Array array = rs.getArray( ALLOWED_EMAIL_DOMAINS.getName() );
        if ( array == null ) {
            return null;
        }
        String[] value = (String[]) array.getArray();
        return DelegatedStringSet.wrap( Sets.newHashSet( value ) );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    public Map<UUID, DelegatedStringSet> loadAll( Collection<UUID> keys ) {
        Map<UUID, DelegatedStringSet> result = Maps.newConcurrentMap();
        keys.parallelStream().forEach( id -> {
            DelegatedStringSet domains = load( id );
            if ( domains != null )
                result.put( id, domains );
        } );
        return result;
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, ALLOWED_EMAIL_DOMAINS );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public DelegatedStringSet generateTestValue() {
        return DelegatedStringSet.wrap( ImmutableSet.of( RandomStringUtils.randomAlphanumeric( 5 ),
                RandomStringUtils.randomAlphanumeric( 5 ),
                RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }
}
