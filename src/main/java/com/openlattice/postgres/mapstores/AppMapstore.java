package com.openlattice.postgres.mapstores;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.apps.App;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.lang3.RandomStringUtils;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.UUID;

public class AppMapstore extends AbstractBasePostgresMapstore<UUID, App> {

    private static final ObjectMapper mapper = ObjectMappers.getJsonMapper();

    public AppMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APPS.name(), PostgresTable.APPS, hds );
    }

    @Override protected void bind( PreparedStatement ps, UUID key, App value ) throws SQLException {

        String rolesAsString = "[]";
        String settingsAsString = "{}";
        try {
            rolesAsString = mapper.writeValueAsString( value.getAppRoles() );
            settingsAsString = mapper.writeValueAsString( value.getDefaultSettings() );
        } catch ( JsonProcessingException e ) {
            logger.error( "Unable to write AppRoles as string for app {}: {}", key, value, e );
        }

        int index = bind( ps, key, 1 );
        ps.setString( index++, value.getName() );
        ps.setString( index++, value.getTitle() );
        ps.setString( index++, value.getDescription() );
        ps.setObject( index++, value.getEntityTypeCollectionId() );
        ps.setString( index++, value.getUrl() );
        ps.setString( index++, rolesAsString );
        ps.setString( index++, settingsAsString );

        // UPDATE
        ps.setString( index++, value.getName() );
        ps.setString( index++, value.getTitle() );
        ps.setString( index++, value.getDescription() );
        ps.setObject( index++, value.getEntityTypeCollectionId() );
        ps.setString( index++, value.getUrl() );
        ps.setString( index++, rolesAsString );
        ps.setString( index++, settingsAsString );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected App mapToValue( ResultSet rs ) throws SQLException {
        try {
            return ResultSetAdapters.app( rs );
        } catch ( IOException e ) {
            logger.error( "Could not deserialize app from ResultSet", e );
            return null;
        }
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public App generateTestValue() {
        return TestDataFactory.app();
    }
}
