package com.openlattice.postgres.mapstores;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.authorization.Permission;
import com.openlattice.hazelcast.HazelcastMap;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.UUID;

public class AppConfigMapstore extends AbstractBasePostgresMapstore<AppConfigKey, AppTypeSetting> {
    public static final String APP_ID          = "__key#appId";
    public static final String ORGANIZATION_ID = "__key#organizationId";

    private static final ObjectMapper mapper = ObjectMappers.getJsonMapper();

    public AppConfigMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APP_CONFIGS.name(), PostgresTable.APP_CONFIGS, hds );
    }

    @Override protected void bind( PreparedStatement ps, AppConfigKey key, AppTypeSetting value ) throws SQLException {
        int index = bind( ps, key, 1 );

        String rolesAsString = "[]";
        String settingsAsString = "{}";
        try {
            rolesAsString = mapper.writeValueAsString( value.getRoles() );
            settingsAsString = mapper.writeValueAsString( value.getSettings() );
        } catch ( JsonProcessingException e ) {
            logger.error( "Unable to write roles as string for AppConfigKey {} with roles {}",
                    key,
                    value.getRoles(),
                    e );
        }

        ps.setObject( index++, value.getId() );
        ps.setObject( index++, value.getEntitySetCollectionId() );
        ps.setString( index++, rolesAsString );
        ps.setString( index++, settingsAsString );

        // UPDATE
        ps.setObject( index++, value.getId() );
        ps.setObject( index++, value.getEntitySetCollectionId() );
        ps.setString( index++, rolesAsString );
        ps.setString( index++, settingsAsString );
    }

    @Override protected int bind( PreparedStatement ps, AppConfigKey key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key.getAppId() );
        ps.setObject( parameterIndex++, key.getOrganizationId() );
        return parameterIndex;
    }

    @Override protected AppTypeSetting mapToValue( ResultSet rs ) throws SQLException {
        try {
            return ResultSetAdapters.appTypeSetting( rs );
        } catch ( IOException e ) {
            logger.error( "Unable to deserialize AppConfigSetting", e );
            return null;
        }
    }

    @Override protected AppConfigKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.appConfigKey( rs );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return super.getMapStoreConfig()
                .setInitialLoadMode( MapStoreConfig.InitialLoadMode.EAGER );
    }

    @Override public MapConfig getMapConfig() {
        return super.getMapConfig()
                .addMapIndexConfig( new MapIndexConfig( APP_ID, false ) )
                .addMapIndexConfig( new MapIndexConfig( ORGANIZATION_ID, false ) );
    }

    @Override public AppConfigKey generateTestKey() {
        return TestDataFactory.appConfigKey();
    }

    @Override public AppTypeSetting generateTestValue() {
        return TestDataFactory.appConfigSetting();
    }
}
