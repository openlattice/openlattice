package com.openlattice.postgres.mapstores;

import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.authorization.Permission;
import com.openlattice.hazelcast.HazelcastMap;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.UUID;

public class AppConfigMapstore extends AbstractBasePostgresMapstore<AppConfigKey, AppTypeSetting> {
    public static final String APP_ID = "__key#appId";
    public static final String ORGANIZATION_ID = "__key#organizationId";

    public AppConfigMapstore( HikariDataSource hds ) {
        super( HazelcastMap.APP_CONFIGS.name(), PostgresTable.APP_CONFIGS, hds );
    }

    @Override protected void bind( PreparedStatement ps, AppConfigKey key, AppTypeSetting value ) throws SQLException {
        bind( ps, key, 1 );

        Array permissions = PostgresArrays.createTextArray( ps.getConnection(),
                value.getPermissions().stream().map( permission -> permission.toString() ) );

        ps.setArray( 4, permissions );
        ps.setObject( 5, value.getEntitySetId() );

        // UPDATE
        ps.setArray( 6, permissions );
        ps.setObject( 7, value.getEntitySetId() );
    }

    @Override protected int bind( PreparedStatement ps, AppConfigKey key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key.getAppId() );
        ps.setObject( parameterIndex++, key.getOrganizationId() );
        ps.setObject( parameterIndex++, key.getAppTypeId() );
        return parameterIndex;
    }

    @Override protected AppTypeSetting mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.appTypeSetting( rs );
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
        return new AppConfigKey( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public AppTypeSetting generateTestValue() {
        return new AppTypeSetting( UUID.randomUUID(), EnumSet.of( Permission.READ, Permission.WRITE ) );
    }
}
