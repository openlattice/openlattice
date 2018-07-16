package com.openlattice.postgres.mapstores;

import com.openlattice.apps.App;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.UUID;

public class AppMapstore extends AbstractBasePostgresMapstore<UUID, App> {
    public AppMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APPS.name(), PostgresTable.APPS, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, App value ) throws SQLException {
        bind( ps, key, 1 );
        ps.setString( 2, value.getName() );
        ps.setString( 3, value.getTitle() );
        ps.setString( 4, value.getDescription() );
        Array configTypesArray = PostgresArrays
                .createUuidArray( ps.getConnection(), value.getAppTypeIds().stream() );
        ps.setArray( 5, configTypesArray );
        ps.setString( 6, value.getUrl() );

        // UPDATE
        ps.setString( 7, value.getName() );
        ps.setString( 8, value.getTitle() );
        ps.setString( 9, value.getDescription() );
        ps.setArray( 10, configTypesArray );
        ps.setString( 11, value.getUrl() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected App mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.app( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public App generateTestValue() {
        LinkedHashSet<UUID> configIds = new LinkedHashSet<>();
        configIds.add( UUID.randomUUID() );
        return new App( UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric( 5 ),
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) ),
                configIds,
                RandomStringUtils.randomAlphabetic( 5 ) );
    }
}
