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

package com.openlattice.datastore.apps.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.openlattice.apps.App;
import com.openlattice.apps.AppApi;
import com.openlattice.apps.AppConfig;
import com.openlattice.apps.AppType;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.organization.Organization;
import com.openlattice.organizations.HazelcastOrganizationService;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping( AppApi.CONTROLLER )
public class AppController implements AppApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private AppService appService;

    @Inject
    private HazelcastOrganizationService organizations;

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<App> getApps() {
        return appService.getApps();
    }

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createApp( @RequestBody App app ) {
        ensureAdminAccess();
        return appService.createApp( app );
    }

    @Timed
    @Override
    @RequestMapping(
            path = TYPE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createAppType( @RequestBody AppType appType ) {
        ensureAdminAccess();
        return appService.createAppType( appType );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public App getApp( @PathVariable( ID ) UUID id ) {
        return appService.getApp( id );
    }

    @Timed
    @Override
    @RequestMapping(
            path = LOOKUP_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public App getApp( @PathVariable( NAME ) String name ) {
        return appService.getApp( name );
    }

    @Timed
    @Override
    @RequestMapping(
            path = TYPE_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public AppType getAppType( @PathVariable( ID ) UUID id ) {
        return appService.getAppType( id );
    }

    @Timed
    @Override
    @RequestMapping(
            path = TYPE_PATH + LOOKUP_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public AppType getAppType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return appService.getAppType( new FullQualifiedName( namespace, name ) );
    }

    @Timed
    @Override
    @RequestMapping(
            path = TYPE_PATH + BULK_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<UUID, AppType> getAppTypes( @RequestBody Set<UUID> appTypeIds ) {
        return appService.getAppTypes( appTypeIds );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public void deleteApp( @PathVariable( ID ) UUID id ) {
        ensureAdminAccess();
        appService.deleteApp( id );
    }

    @Timed
    @Override
    @RequestMapping(
            path = TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public void deleteAppType( @PathVariable( ID ) UUID id ) {
        ensureAdminAccess();
        appService.deleteAppType( id );
    }

    @Timed
    @Override
    @RequestMapping(
            path = INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH + PREFIX_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public void installApp(
            @PathVariable( ID ) UUID appId,
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( PREFIX ) String prefix ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.installApp( appId, organizationId, prefix, Principals.getCurrentUser() );
    }

    private Iterable<Organization> getAvailableOrgs() {
        return getAccessibleObjects( SecurableObjectType.Organization,
                EnumSet.of( Permission.READ ) )
                .filter( Predicates.notNull()::apply ).map( AuthorizationUtils::getLastAclKeySafely )
                .map( organizations::getOrganization )
                .filter( organization -> organization != null )::iterator;
    }

    @Timed
    @Override
    @RequestMapping(
            path = CONFIG_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public List<AppConfig> getAvailableAppConfigs( @PathVariable( ID ) UUID appId ) {
        Iterable<Organization> orgs = getAvailableOrgs();
        return appService.getAvailableConfigs( appId, Principals.getCurrentPrincipals(), orgs );
    }

    @Timed
    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_TYPE_ID_PATH,
            method = RequestMethod.GET )
    public void addAppTypeToApp( @PathVariable( ID ) UUID appId, @PathVariable( APP_TYPE_ID ) UUID appTypeId ) {
        ensureAdminAccess();
        appService.addAppTypesToApp( appId, ImmutableSet.of( appTypeId ) );
    }

    @Timed
    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    public void removeAppTypeFromApp( @PathVariable( ID ) UUID appId, @PathVariable( APP_TYPE_ID ) UUID appTypeId ) {
        ensureAdminAccess();
        appService.removeAppTypesFromApp( appId, ImmutableSet.of( appTypeId ) );
    }

    @Timed
    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_ID_PATH + APP_TYPE_ID_PATH + ENTITY_SET_ID_PATH,
            method = RequestMethod.GET )
    public void updateAppEntitySetConfig(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( APP_ID ) UUID appId,
            @PathVariable( APP_TYPE_ID ) UUID appTypeId,
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.updateAppConfigEntitySetId( organizationId, appId, appTypeId, entitySetId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_ID_PATH + APP_TYPE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppEntitySetPermissionsConfig(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( APP_ID ) UUID appId,
            @PathVariable( APP_TYPE_ID ) UUID appTypeId,
            @RequestBody Set<Permission> permissions ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.updateAppConfigPermissions( organizationId, appId, appTypeId, EnumSet.copyOf( permissions ) );
    }

    @Timed
    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppMetadata(
            @PathVariable( ID ) UUID appId,
            @RequestBody MetadataUpdate metadataUpdate ) {
        ensureAdminAccess();
        appService.updateAppMetadata( appId, metadataUpdate );
    }

    @Timed
    @Override
    @RequestMapping(
            path = TYPE_PATH + UPDATE_PATH + ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppTypeMetadata(
            @PathVariable( ID ) UUID appTypeId,
            @RequestBody MetadataUpdate metadataUpdate ) {
        ensureAdminAccess();
        appService.updateAppTypeMetadata( appTypeId, metadataUpdate );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
