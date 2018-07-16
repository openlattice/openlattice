package com.openlattice.postgres.mapstores;

import com.openlattice.apps.AppType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class AppTypeMapstore extends AbstractBasePostgresMapstore<UUID, AppType> {
    public AppTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APP_TYPES.name(), PostgresTable.APP_TYPES, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, AppType value ) throws SQLException {
        bind( ps, key, 1 );

        ps.setString( 2, value.getType().getNamespace() );
        ps.setString( 3, value.getType().getName() );
        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );
        ps.setObject( 6, value.getEntityTypeId() );

        // UPDATE
        ps.setString( 7, value.getType().getNamespace() );
        ps.setString( 8, value.getType().getName() );
        ps.setString( 9, value.getTitle() );
        ps.setString( 10, value.getDescription() );
        ps.setObject( 11, value.getEntityTypeId() );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected AppType mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.appType( rs );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public AppType generateTestValue() {
        return new AppType( UUID.randomUUID(),
                new FullQualifiedName( RandomStringUtils.randomAlphanumeric( 5 ),
                        RandomStringUtils.randomAlphanumeric( 5 ) ),
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) ),
                UUID.randomUUID() );
    }
}
