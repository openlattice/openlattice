package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.NAME_SET;
import static com.openlattice.postgres.PostgresTable.SCHEMA;

import com.openlattice.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.lang3.RandomStringUtils;

public class SchemasMapstore extends AbstractBasePostgresMapstore<String, DelegatedStringSet> {

    public SchemasMapstore( HikariDataSource hds ) {
        super( HazelcastMap.SCHEMAS.name(), SCHEMA, hds );
    }

    @Override protected void bind(
            PreparedStatement ps, String key, DelegatedStringSet value ) throws SQLException {
        bind( ps, key, 1 );

        Array names = PostgresArrays.createTextArray( ps.getConnection(), value.stream() );
        ps.setArray( 2, names );

        // UPDATE
        ps.setArray( 3, names );
    }

    @Override protected int bind( PreparedStatement ps, String key, int parameterIndex ) throws SQLException {
        ps.setString( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected DelegatedStringSet mapToValue( ResultSet rs ) throws SQLException {
        return DelegatedStringSet.wrap( Sets.newHashSet( (String[]) rs.getArray( NAME_SET.getName() ).getArray() ) );
    }

    @Override protected String mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.namespace( rs );
    }

    @Override public String generateTestKey() {
        return RandomStringUtils.randomAlphanumeric( 5 );
    }

    @Override public DelegatedStringSet generateTestValue() {
        return DelegatedStringSet.wrap( ImmutableSet.of( RandomStringUtils.randomAlphanumeric( 5 ),
                RandomStringUtils.randomAlphanumeric( 5 ),
                RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }
}
