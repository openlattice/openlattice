package com.openlattice.postgres.mapstores;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.APP_IDS;
import static com.openlattice.postgres.PostgresColumn.ID;

public class OrganizationAppsMapstore extends AbstractBasePostgresMapstore<UUID, DelegatedUUIDSet> {

    public OrganizationAppsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ORGANIZATION_APPS.name(), PostgresTable.ORGANIZATIONS, hds );
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( APP_IDS );
    }

    @Override protected void bind(
            PreparedStatement ps, UUID key, DelegatedUUIDSet value ) throws SQLException {
        bind( ps, key, 1);

        Array arr = PostgresArrays.createUuidArray( ps.getConnection(), value.stream() );

        ps.setArray( 2, arr );

        // UPDATE
        ps.setArray( 3, arr );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected DelegatedUUIDSet mapToValue( ResultSet rs ) throws SQLException {
        Array arr = rs.getArray( APP_IDS.getName() );
        if ( arr != null ) {
            UUID[] value = (UUID[]) arr.getArray();
            if ( value != null )
                return DelegatedUUIDSet.wrap( Sets.newHashSet( value ) );
        }
        return DelegatedUUIDSet.wrap( Sets.newHashSet() );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    public Map<UUID, DelegatedUUIDSet> loadAll( Collection<UUID> keys ) {
        Map<UUID, DelegatedUUIDSet> result = Maps.newConcurrentMap();
        keys.parallelStream().forEach( id -> {
            DelegatedUUIDSet apps = load( id );
            if ( apps != null )
                result.put( id, apps );
        } );
        return result;
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, APP_IDS );
    }


    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public DelegatedUUIDSet generateTestValue() {
        return DelegatedUUIDSet.wrap( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) );
    }
}
