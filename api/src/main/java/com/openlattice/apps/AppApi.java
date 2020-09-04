package com.openlattice.apps;

import com.openlattice.apps.historical.HistoricalAppConfig;
import com.openlattice.authorization.Permission;
import com.openlattice.edm.requests.MetadataUpdate;
import retrofit2.http.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface AppApi {

    String SERVICE    = "/datastore";
    String CONTROLLER = "/app";
    String BASE       = SERVICE + CONTROLLER;

    String INSTALL_PATH      = "/install";
    String CONFIG_PATH       = "/config";
    String LOOKUP_PATH       = "/lookup";
    String ORGANIZATION_PATH = "/organization";
    String UPDATE_PATH       = "/update";
    String ROLE_PATH         = "/role";

    String ID              = "id";
    String ORGANIZATION_ID = "organizationId";
    String NAME            = "name";
    String NAMESPACE       = "namespace";
    String ROLE_ID         = "roleId";

    String ID_PATH              = "/{" + ID + "}";
    String ORGANIZATION_ID_PATH = "/{" + ORGANIZATION_ID + "}";
    String NAME_PATH            = "/{" + NAME + "}";
    String ROLE_ID_PATH         = "/{" + ROLE_ID + "}";

    /**
     * App CRUD
     **/

    @GET( BASE )
    Iterable<App> getApps();

    @POST( BASE )
    UUID createApp( @Body App app );

    @GET( BASE + ID_PATH )
    App getApp( @Path( ID ) UUID id );

    @GET( BASE + LOOKUP_PATH + NAME_PATH )
    App getAppByName( @Path( NAME ) String name );

    @DELETE( BASE + ID_PATH )
    void deleteApp( @Path( ID ) UUID id );

    @POST( BASE + UPDATE_PATH + ID_PATH + ROLE_PATH )
    UUID createAppRole( @Path( ID ) UUID appId, @Body AppRole role );

    @DELETE( BASE + UPDATE_PATH + ID_PATH + ROLE_PATH + ROLE_ID_PATH )
    void deleteRoleFromApp( @Path( ID ) UUID appId, @Path( ROLE_ID ) UUID roleId );

    @POST( BASE + UPDATE_PATH + ID_PATH + ROLE_ID_PATH )
    void updateAppEntitySetPermissionsConfig(
            @Path( ID ) UUID appId,
            @Path( ROLE_ID ) UUID roleId,
            @Body Map<Permission, Map<UUID, Optional<Set<UUID>>>> requiredPermissions );

    @POST( BASE + UPDATE_PATH + ID_PATH )
    void updateAppMetadata( @Path( ID ) UUID appId, @Body MetadataUpdate metadataUpdate );

    @PATCH( BASE + UPDATE_PATH + ID_PATH )
    void updateDefaultAppSettings( @Path( ID ) UUID appId, @Body Map<String, Object> defaultSettings );

    /**
     * App Installation CRUD
     **/

    @POST( BASE + INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH )
    void installApp(
            @Path( ID ) UUID appId,
            @Path( ORGANIZATION_ID ) UUID organizationId,
            @Body AppInstallation appInstallation );

    @DELETE( BASE + INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH )
    void uninstallApp( @Path( ID ) UUID appId, @Path( ORGANIZATION_ID ) UUID organizationId );

    @GET( BASE + CONFIG_PATH + ID_PATH )
    @Deprecated
    List<HistoricalAppConfig> getAvailableAppConfigsOld( @Path( ID ) UUID appId );

    @GET( BASE + CONFIG_PATH )
    List<UserAppConfig> getAvailableAppConfigs( @Query( ID ) UUID appId );

    @GET( BASE + ORGANIZATION_PATH + ORGANIZATION_ID_PATH )
    Map<UUID, AppTypeSetting> getOrganizationAppsByAppId( @Path( ORGANIZATION_ID ) UUID organizationId );

    @POST( BASE + CONFIG_PATH + UPDATE_PATH + ID_PATH + ORGANIZATION_ID_PATH )
    void updateAppConfigSettings(
            @Path( ID ) UUID appId,
            @Path( ORGANIZATION_ID ) UUID organizationId,
            @Body Map<String, Object> newSettings );
}
