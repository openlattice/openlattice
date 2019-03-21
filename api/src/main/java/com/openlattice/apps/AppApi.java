package com.openlattice.apps;

import com.openlattice.authorization.Permission;
import com.openlattice.edm.requests.MetadataUpdate;
import retrofit2.http.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface AppApi {

    String SERVICE    = "/datastore";
    String CONTROLLER = "/app";
    String BASE       = SERVICE + CONTROLLER;

    String INSTALL_PATH = "/install";
    String TYPE_PATH    = "/type";
    String CONFIG_PATH  = "/config";
    String BULK_PATH    = "/bulk";
    String LOOKUP_PATH  = "/lookup";
    String UPDATE_PATH  = "/update";

    String ID              = "id";
    String ORGANIZATION_ID = "organizationId";
    String PREFIX          = "prefix";
    String NAME            = "name";
    String NAMESPACE       = "namespace";
    String APP_ID          = "appId";
    String APP_TYPE_ID     = "appTypeId";
    String ENTITY_SET_ID   = "entitySetId";

    String ID_PATH              = "/{" + ID + "}";
    String ORGANIZATION_ID_PATH = "/{" + ORGANIZATION_ID + "}";
    String PREFIX_PATH          = "/{" + PREFIX + "}";
    String NAME_PATH            = "/{" + NAME + "}";
    String NAMESPACE_PATH       = "/{" + NAMESPACE + "}";
    String APP_ID_PATH          = "/{" + APP_ID + "}";
    String APP_TYPE_ID_PATH     = "/{" + APP_TYPE_ID + "}";
    String ENTITY_SET_ID_PATH   = "/{" + ENTITY_SET_ID + "}";

    @GET( BASE )
    Iterable<App> getApps();

    @GET( BASE + TYPE_PATH )
    Iterable<AppType> getAppTypes();

    @POST( BASE )
    UUID createApp( @Body App app );

    @POST( BASE + TYPE_PATH )
    UUID createAppType( @Body AppType appType );

    @GET( BASE + ID_PATH )
    App getApp( @Path( ID ) UUID id );

    @GET( BASE + LOOKUP_PATH + NAME_PATH )
    App getApp( @Path( NAME ) String name );

    @GET( BASE + TYPE_PATH + ID_PATH )
    AppType getAppType( @Path( ID ) UUID id );

    @GET( BASE + TYPE_PATH + LOOKUP_PATH + NAMESPACE_PATH + NAME_PATH )
    AppType getAppType( @Path( NAMESPACE ) String namespace, @Path( NAME ) String name );

    @POST( BASE + TYPE_PATH + BULK_PATH )
    Map<UUID, AppType> getAppTypes( @Body Set<UUID> appTypeIds );

    @DELETE( BASE + ID_PATH )
    void deleteApp( @Path( ID ) UUID id );

    @DELETE( BASE + TYPE_PATH + ID_PATH )
    void deleteAppType( @Path( ID ) UUID id );

    @GET( BASE + INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH + PREFIX_PATH )
    void installApp(
            @Path( ID ) UUID appId,
            @Path( ORGANIZATION_ID ) UUID organizationId,
            @Path( PREFIX ) String prefix );

    @GET( BASE + CONFIG_PATH + ID_PATH )
    List<AppConfig> getAvailableAppConfigs( @Path( ID ) UUID appId );

    @POST( BASE + UPDATE_PATH + ID_PATH + APP_TYPE_ID_PATH )
    void addAppTypeToApp( @Path( ID ) UUID appId, @Path( APP_TYPE_ID ) UUID appTypeId );

    @DELETE( BASE + UPDATE_PATH + ID_PATH + APP_TYPE_ID_PATH )
    void removeAppTypeFromApp( @Path( ID ) UUID appId, @Path( APP_TYPE_ID ) UUID appTypeId );

    @GET( BASE + UPDATE_PATH + ID_PATH + APP_ID_PATH + APP_TYPE_ID_PATH + ENTITY_SET_ID_PATH )
    void updateAppEntitySetConfig(
            @Path( ID ) UUID organizationId,
            @Path( APP_ID ) UUID appId,
            @Path( APP_TYPE_ID ) UUID appTypeId,
            @Path( ENTITY_SET_ID ) UUID entitySetId );

    @POST( BASE + UPDATE_PATH + ID_PATH + APP_ID_PATH + APP_TYPE_ID_PATH )
    void updateAppEntitySetPermissionsConfig(
            @Path( ID ) UUID organizationId,
            @Path( APP_ID ) UUID appId,
            @Path( APP_TYPE_ID ) UUID appTypeId,
            @Body Set<Permission> permissions );

    @POST( BASE + UPDATE_PATH + ID_PATH )
    void updateAppMetadata( @Path( ID ) UUID appId, @Body MetadataUpdate metadataUpdate );

    @POST( BASE + TYPE_PATH + UPDATE_PATH + ID_PATH )
    void updateAppTypeMetadata( @Path( ID ) UUID appTypeId, @Body MetadataUpdate metadataUpdate );

}
