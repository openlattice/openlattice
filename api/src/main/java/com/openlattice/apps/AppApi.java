package com.openlattice.apps;

import com.openlattice.authorization.Permission;
import com.openlattice.edm.requests.MetadataUpdate;
import retrofit2.http.*;

import java.util.*;

public interface AppApi {

    String SERVICE    = "/datastore";
    String CONTROLLER = "/app";
    String BASE       = SERVICE + CONTROLLER;

    String INSTALL_PATH = "/install";
    String CONFIG_PATH  = "/config";
    String BULK_PATH    = "/bulk";
    String LOOKUP_PATH  = "/lookup";
    String UPDATE_PATH  = "/update";
    String ROLE_PATH    = "/role";

    String ENTITY_SET_COLLECTION_ID = "entitySetCollectionId";
    String ID                       = "id";
    String ORGANIZATION_ID          = "organizationId";
    String PREFIX                   = "prefix";
    String NAME                     = "name";
    String NAMESPACE                = "namespace";
    String APP_ID                   = "appId";
    String ROLE_ID                  = "roleId";

    String ENTITY_SET_COLLECTION_ID_PATH = "/{" + ENTITY_SET_COLLECTION_ID + "}";
    String ID_PATH                       = "/{" + ID + "}";
    String ORGANIZATION_ID_PATH          = "/{" + ORGANIZATION_ID + "}";
    String PREFIX_PATH                   = "/{" + PREFIX + "}";
    String NAME_PATH                     = "/{" + NAME + "}";
    String NAMESPACE_PATH                = "/{" + NAMESPACE + "}";
    String APP_ID_PATH                   = "/{" + APP_ID + "}";
    String ROLE_ID_PATH                  = "/{" + ROLE_ID + "}";

    /**
     * App CRUD
     **/

    @GET( BASE )
    Iterable<App> getApps();

    @POST( BASE )
    UUID createApp( @Body App app );

    @POST( BASE )
    List<UUID> createApps( @Body List<App> app );

    @GET( BASE + ID_PATH )
    App getApp( @Path( ID ) UUID id );

    @GET( BASE + LOOKUP_PATH + NAME_PATH )
    App getApp( @Path( NAME ) String name );

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

    @POST( BASE + INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH + PREFIX_PATH )
    void installApp(
            @Path( ID ) UUID appId,
            @Path( ORGANIZATION_ID ) UUID organizationId,
            @Body AppInstallation appInstallation );

    @DELETE( BASE + INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH )
    void uninstallApp( @Path( ID ) UUID appId, @Path( ORGANIZATION_ID ) UUID organizationId );

    @GET( BASE + CONFIG_PATH + ID_PATH )
    List<UserAppConfig> getAvailableAppConfigs( @Path( ID ) UUID appId );

    @POST( BASE + CONFIG_PATH + UPDATE_PATH + ID_PATH + ORGANIZATION_ID_PATH )
    void updateAppConfigSettings(
            @Path( ID ) UUID appId,
            @Path( ORGANIZATION_ID ) UUID organizationId,
            @Body Map<String, Object> newSettings );
}
