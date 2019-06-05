/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.datastore.apps.services;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.openlattice.apps.*;
import com.openlattice.apps.processors.*;
import com.openlattice.authorization.*;
import com.openlattice.collections.CollectionsManager;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.util.Util;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.collection.CollectionTemplateType;
import com.openlattice.edm.collection.EntitySetCollection;
import com.openlattice.edm.collection.EntityTypeCollection;
import com.openlattice.edm.events.AppCreatedEvent;
import com.openlattice.edm.events.AppDeletedEvent;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.type.EntityType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.mapstores.AppConfigMapstore;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AppService {
    private final IMap<UUID, App>                    apps;
    private final IMap<AppConfigKey, AppTypeSetting> appConfigs;
    private final IMap<String, UUID>                 aclKeys;
    private final IMap<UUID, EntityTypeCollection>   entityTypeCollections;
    private final IMap<UUID, EntitySetCollection>    entitySetCollections;

    private final EdmManager                        edmService;
    private final HazelcastOrganizationService      organizationService;
    private final AuthorizationQueryService         authorizations;
    private final AuthorizationManager              authorizationService;
    private final SecurePrincipalsManager           principalsService;
    private final HazelcastAclKeyReservationService reservations;
    private final CollectionsManager                collectionsManager;

    @Inject
    private EventBus eventBus;

    public AppService(
            HazelcastInstance hazelcast,
            EdmManager edmService,
            HazelcastOrganizationService organizationService,
            AuthorizationQueryService authorizations,
            AuthorizationManager authorizationService,
            SecurePrincipalsManager principalsService,
            HazelcastAclKeyReservationService reservations,
            CollectionsManager collectionsManager
    ) {
        this.apps = hazelcast.getMap( HazelcastMap.APPS.name() );
        this.appConfigs = hazelcast.getMap( HazelcastMap.APP_CONFIGS.name() );
        this.aclKeys = hazelcast.getMap( HazelcastMap.ACL_KEYS.name() );
        this.entityTypeCollections = hazelcast.getMap( HazelcastMap.ENTITY_TYPE_COLLECTIONS.name() );
        this.entitySetCollections = hazelcast.getMap( HazelcastMap.ENTITY_SET_COLLECTIONS.name() );

        this.edmService = edmService;
        this.organizationService = organizationService;
        this.authorizations = authorizations;
        this.authorizationService = authorizationService;
        this.principalsService = principalsService;
        this.reservations = reservations;
        this.collectionsManager = collectionsManager;
    }

    public Iterable<App> getApps() {
        return apps.values();
    }

    public UUID createApp( App app ) {
        reservations.reserveIdAndValidateType( app, app::getName );
        apps.put( app.getId(), app );
        eventBus.post( new AppCreatedEvent( app ) );
        return app.getId();
    }

    public void deleteApp( UUID appId ) {
        appConfigs.keySet( Predicates.equal( AppConfigMapstore.APP_ID, appId ) )
                .forEach( ack -> uninstallApp( ack.getAppId(), ack.getOrganizationId() ) );
        apps.delete( appId );
        reservations.release( appId );
        eventBus.post( new AppDeletedEvent( appId ) );
    }

    public App getApp( UUID appId ) {
        App app = apps.get( appId );
        Preconditions.checkNotNull( app, "App with id {} does not exist.", appId );
        return app;
    }

    public App getApp( String name ) {
        UUID id = Util.getSafely( aclKeys, name );
        return getApp( id );
    }

    public UUID createNewAppRole( UUID appId, AppRole role ) {
        App app = getApp( appId );

        apps.executeOnKey( appId, new AddRoleToAppProcessor( role ) );
    }

    public void deleteRoleFromApp( UUID appId, UUID roleId ) {
        App app = getApp( appId );
        appConfigs.executeOnKeys( getAppConfigKeysForApp(appId ), new RemoveRoleFromAppConfigProcessor( roleId ) );
        apps.executeOnKey( appId, new RemoveRoleFromAppProcessor( roleId ) );
    }

    private Set<AppConfigKey> getAppConfigKeysForApp( UUID appId ) {
        return appConfigs.keySet( Predicates.equal( AppConfigMapstore.APP_ID, appId ) );
    }

    private Map<UUID, AclKey> createRolesForApp(
            Set<AppRole> appRoles,
            UUID organizationId,
            UUID entitySetCollectionId,
            Principal appPrincipal,
            Principal userPrincipal ) {
        Map<AceKey, EnumSet<Permission>> permissionsToGrant = Maps.newHashMap();

        EntitySetCollection entitySetCollection = collectionsManager.getEntitySetCollection( entitySetCollectionId );
        EntityTypeCollection entityTypeCollection = collectionsManager
                .getEntityTypeCollection( entitySetCollection.getEntityTypeCollectionId() );
        Map<UUID, EntityType> entityTypesById = edmService
                .getEntityTypesAsMap( entityTypeCollection.getTemplate().stream()
                        .map( CollectionTemplateType::getEntityTypeId ).collect(
                                Collectors.toSet() ) );
        Map<UUID, EntitySet> entitySetsById = edmService
                .getEntitySetsAsMap( Sets.newHashSet( entitySetCollection.getTemplate().values() ) );

        Map<UUID, AclKey> roles = appRoles.stream().collect( Collectors.toMap( AppRole::getId, appRole -> {

            /* Create the role if it doesn't already exist */
            String title = appRole.getTitle();
            Principal rolePrincipal = new Principal( PrincipalType.ROLE, organizationId.toString() + "|" + title );
            String description = appRole.getDescription();
            Role role = new Role( Optional.empty(), organizationId, rolePrincipal, title, Optional.of( description ) );

            AclKey aclKey = principalsService.createSecurablePrincipalIfNotExists( userPrincipal, role ) ?
                    role.getAclKey() :
                    principalsService.lookup( rolePrincipal );

            /* Track permissions that need to be granted to the role */
            permissionsToGrant
                    .put( new AceKey( new AclKey( entitySetCollectionId ), rolePrincipal ),
                            EnumSet.of( Permission.READ ) );
            permissionsToGrant
                    .put( new AceKey( new AclKey( entitySetCollectionId ), appPrincipal ),
                            EnumSet.of( Permission.READ ) );
            appRole.getPermissions().entrySet().forEach( entry -> {
                Permission permission = entry.getKey();
                entry.getValue().entrySet().forEach( entitySetEntry -> {
                    UUID entitySetId = entitySetEntry.getKey();
                    Set<UUID> propertyTypeIds = entitySetEntry.getValue()
                            .orElse( entityTypesById.get( entitySetsById.get( entitySetId ).getEntityTypeId() )
                                    .getProperties() );
                    Streams.concat( Stream.of( new AclKey( entitySetId ) ),
                            propertyTypeIds.stream().map( id -> new AclKey( entitySetId, id ) ) ).forEach( ak -> {
                        AceKey aceKey = new AceKey( ak, rolePrincipal );
                        EnumSet<Permission> permissionEnumSet = permissionsToGrant
                                .getOrDefault( aceKey, EnumSet.noneOf( Permission.class ) );
                        permissionEnumSet.add( permission );
                        permissionsToGrant.put( aceKey, permissionEnumSet );
                        permissionsToGrant.put( new AceKey( ak, appPrincipal ), EnumSet.of( Permission.READ ) );
                    } );

                } );
            } );

            return aclKey;

        } ) );

        /* Grant the required permissions to app roles */
        authorizationService.setPermissions( permissionsToGrant );

        return roles;
    }

    public void installAppAndCreateEntitySetCollection(
            UUID appId,
            UUID organizationId,
            AppInstallation appInstallation,
            Principal principal ) {
        App app = getApp( appId );
        Preconditions.checkNotNull( app, "The requested app with id %s does not exists.", appId.toString() );

        UUID entitySetCollectionId = appInstallation.getEntitySetCollectionId()
                .orElse( collectionsManager.createEntitySetCollection( new EntitySetCollection(
                        Optional.empty(),
                        app.getName(),
                        app.getTitle(),
                        Optional.of( app.getDescription() ),
                        app.getEntityTypeCollectionId(),
                        appInstallation.getTemplate().get(),
                        ImmutableSet.of(),
                        Optional.of( organizationId )
                ), true ) );

        Map<String, Object> settings = appInstallation.getSettings().orElse( app.getDefaultSettings() );

        installApp( app, organizationId, entitySetCollectionId, principal, settings );
    }

    public void installApp(
            App app,
            UUID organizationId,
            UUID entitySetCollectionId,
            Principal principal,
            Map<String, Object> settings ) {

        UUID appId = app.getId();

        AppConfigKey appConfigKey = new AppConfigKey( appId, organizationId );
        Preconditions.checkArgument( !appConfigs.containsKey( appConfigKey ),
                "App {} is already installed for organization {}",
                appId,
                organizationId );

        Set<String> nonexistentKeys = Sets.difference( settings.keySet(), app.getDefaultSettings().keySet() );
        Preconditions.checkArgument( nonexistentKeys.isEmpty(),
                "Cannot create app {} in organization {} with settings containing keys that do not exist: {}",
                appId,
                organizationId,
                nonexistentKeys );

        Principal appPrincipal = getAppPrincipal( appConfigKey );

        principalsService.createSecurablePrincipalIfNotExists( principal, new SecurablePrincipal(
                Optional.empty(),
                appPrincipal,
                app.getTitle(),
                Optional.of( app.getDescription() ) ) );

        Map<UUID, AclKey> appRoles = createRolesForApp( app.getAppRoles(),
                organizationId,
                entitySetCollectionId,
                appPrincipal,
                principal );

        appConfigs.put( appConfigKey,
                new AppTypeSetting( principalsService.lookup( appPrincipal ).get( 0 ),
                        entitySetCollectionId,
                        appRoles,
                        settings ) );

        organizationService.addAppToOrg( organizationId, appId );
    }

    private Principal getAppPrincipal( AppConfigKey appConfigKey ) {
        return new Principal( PrincipalType.APP,
                AppConfig.getAppPrincipalId( appConfigKey.getAppId(), appConfigKey.getOrganizationId() ) );
    }

    public void uninstallApp( UUID appId, UUID organizationId ) {
        AppConfigKey appConfigKey = new AppConfigKey( appId, organizationId );

        organizationService.removeAppFromOrg( organizationId, appId );
        deleteAppPrincipal( appConfigKey );
        appConfigs.delete( appConfigKey );
    }

    private void deleteAppPrincipal( AppConfigKey appConfigKey ) {
        principalsService.deletePrincipal( principalsService.lookup( getAppPrincipal( appConfigKey ) ) );
    }

    public List<UserAppConfig> getAvailableConfigs( UUID appId, Set<Principal> principals ) {

        Map<Principal, AclKey> principalAclKeys = principalsService.lookup( principals );

        Map<PrincipalType, Set<AclKey>> aclKeysByPrincipalType = principalAclKeys.entrySet().stream()
                .collect( Collectors.groupingBy( entry -> entry.getKey().getType(),
                        Collectors.mapping( entry -> entry.getValue(), Collectors.toSet() ) ) );

        return appConfigs.entrySet( Predicates.and(
                Predicates.equal( AppConfigMapstore.APP_ID, appId ),
                Predicates.in( AppConfigMapstore.ORGANIZATION_ID,
                        aclKeysByPrincipalType.getOrDefault( PrincipalType.ORGANIZATION, ImmutableSet.of() )
                                .toArray( new UUID[] {} ) ) ) )
                .stream().map( entry -> {

                    UUID organizationId = entry.getKey().getOrganizationId();
                    AppTypeSetting setting = entry.getValue();

                    UUID entitySetCollectionId = setting.getEntitySetCollectionId();

                    Set<UUID> availableRoles = setting.getRoles().entrySet().stream()
                            .filter( roleEntry -> aclKeysByPrincipalType
                                    .getOrDefault( PrincipalType.ROLE, ImmutableSet.of() )
                                    .contains( roleEntry.getValue() ) )
                            .map( Map.Entry::getKey )
                            .collect( Collectors.toSet() );

                    return new UserAppConfig( organizationId, entitySetCollectionId, availableRoles );
                } ).filter( uac -> !uac.getRoles().isEmpty() )
                .collect( Collectors.toList() );
    }

    public void updateAppRolePermissions(
            UUID appId,
            UUID roleId,
            Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions ) {

        App app = getApp( appId );

        Preconditions.checkState( app.getAppRoles().stream().anyMatch( r -> r.getId().equals( roleId ) ),
                "App {} does not contain a role with id {}.",
                appId,
                roleId );
        Set<UUID> templateTypeIds = permissions.values().stream().flatMap( map -> map.keySet().stream() ).collect(
                Collectors.toSet() );
        Set<UUID> nonexistentTemplateTypeIds = Sets.difference( templateTypeIds,
                entityTypeCollections.get( app.getEntityTypeCollectionId() ).getTemplate().stream()
                        .map( CollectionTemplateType::getId ).collect( Collectors.toSet() ) );
        Preconditions.checkState( nonexistentTemplateTypeIds.isEmpty(),
                "Could not update role {} permissions for app {} because the following templateTypeIds are not present in the EntityTypeCollection: ",
                roleId,
                appId,
                nonexistentTemplateTypeIds );

        apps.executeOnKey( appId, new UpdateAppRolePermissionsProcessor( roleId, permissions ) );
    }

    public void updateAppMetadata( UUID appId, MetadataUpdate metadataUpdate ) {
        apps.executeOnKey( appId, new UpdateAppMetadataProcessor( metadataUpdate ) );
        eventBus.post( new AppCreatedEvent( getApp( appId ) ) );
    }

    public void updateDefaultAppSettings( UUID appId, Map<String, Object> defaultSettings ) {

        App app = getApp( appId );
        Map<String, Object> oldSettings = app.getDefaultSettings();

        Set<String> newKeys = Sets.difference( oldSettings.keySet(), defaultSettings.keySet() );
        Set<String> deletedKeys = Sets.difference( defaultSettings.keySet(), oldSettings.keySet() );

        Map<String, Object> settingsToAdd = defaultSettings.entrySet().stream()
                .filter( e -> newKeys.contains( e.getKey() ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );

        Set<AppConfigKey> appConfigKeysToUpdate = appConfigs
                .keySet( Predicates.equal( AppConfigMapstore.APP_ID, appId ) );

        appConfigs.executeOnKeys( appConfigKeysToUpdate,
                new UpdateAppConfigSettingsProcessor( settingsToAdd, deletedKeys ) );

        apps.executeOnKey( appId, new UpdateDefaultAppSettingsProcessor( defaultSettings ) );
    }

    public void updateAppConfigSettings( UUID appId, UUID organizationId, Map<String, Object> newAppSettings ) {

        App app = getApp( appId );

        AppConfigKey appConfigKey = new AppConfigKey( appId, organizationId );
        Preconditions.checkArgument( appConfigs.containsKey( appConfigKey ),
                "App {} is not installed for organization {}.",
                appId,
                organizationId );

        Set<String> nonexistentKeys = Sets.difference( newAppSettings.keySet(), app.getDefaultSettings().keySet() );
        Preconditions.checkArgument( nonexistentKeys.isEmpty(),
                "Cannot update app {} in organization {} with settings containing keys that do not exist: {}",
                appId,
                organizationId,
                nonexistentKeys );

        appConfigs.executeOnKey( appConfigKey,
                new UpdateAppConfigSettingsProcessor( newAppSettings, ImmutableSet.of() ) );

    }

}
