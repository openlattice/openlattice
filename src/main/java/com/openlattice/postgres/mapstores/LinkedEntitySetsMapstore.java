package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_IDS;
import static com.openlattice.postgres.PostgresTable.LINKED_ENTITY_SETS;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class LinkedEntitySetsMapstore extends AbstractBasePostgresMapstore<UUID, DelegatedUUIDSet> {

    public LinkedEntitySetsMapstore( HikariDataSource hds ) {
        super( HazelcastMap.LINKED_ENTITY_SETS.name(), LINKED_ENTITY_SETS, hds );
    }

    @Override protected void bind(
            PreparedStatement ps, UUID key, DelegatedUUIDSet value ) throws SQLException {
        bind( ps, key, 1 );

        Array valueArr = PostgresArrays.createUuidArray( ps.getConnection(), value.stream() );
        ps.setArray( 2, valueArr );

        // UPDATE
        ps.setArray( 3, valueArr );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected DelegatedUUIDSet mapToValue( ResultSet rs ) throws SQLException {
        return DelegatedUUIDSet.wrap( Sets.newHashSet( (UUID[]) rs.getArray( ENTITY_SET_IDS.getName() ).getArray() ) );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public DelegatedUUIDSet generateTestValue() {
        return DelegatedUUIDSet.wrap( ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() ) );
    }
}
