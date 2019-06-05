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
import com.openlattice.apps.*;
import com.openlattice.authorization.*;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.organizations.HazelcastOrganizationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import retrofit2.http.Path;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

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

    //    @Timed
    //    @Override
    //    @RequestMapping(
    //            method = RequestMethod.POST,
    //            consumes = MediaType.APPLICATION_JSON_VALUE,
    //            produces = MediaType.APPLICATION_JSON_VALUE )
    //    @ResponseStatus( HttpStatus.OK )
    public UUID createApp( @RequestBody App app ) {
        ensureAdminAccess();
        return appService.createApp( app );
    }

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public List<UUID> createApps( @RequestBody List<App> apps ) {
        ensureAdminAccess();
        return apps.stream().map( appService::createApp ).collect( Collectors.toList() );
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
            path = UPDATE_PATH + ID_PATH + ROLE_PATH,
            method = RequestMethod.POST )
    @ResponseStatus( HttpStatus.OK )
    public UUID createAppRole( @PathVariable( ID ) UUID appId, @RequestBody AppRole role ) {
        ensureAdminAccess();
        return appService.createNewAppRole( appId, role );
    }

    @Timed
    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + ROLE_PATH + ROLE_ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public void deleteRoleFromApp( @PathVariable( ID ) UUID appId, @PathVariable( ROLE_ID ) UUID roleId ) {
        ensureAdminAccess();
        appService.deleteRoleFromApp( appId, roleId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH,
            method = RequestMethod.POST )
    @ResponseStatus( HttpStatus.OK )
    public void installApp(
            @PathVariable( ID ) UUID appId,
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @RequestBody AppInstallation appInstallation ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.installAppAndCreateEntitySetCollection( appId,
                organizationId,
                appInstallation,
                Principals.getCurrentUser() );
    }

    @Timed
    @Override
    @RequestMapping(
            path = INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public void uninstallApp(
            @PathVariable( ID ) UUID appId,
            @PathVariable( ORGANIZATION_ID ) UUID organizationId ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.uninstallApp( appId, organizationId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = CONFIG_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public List<UserAppConfig> getAvailableAppConfigs( @PathVariable( ID ) UUID appId ) {
        return appService.getAvailableConfigs( appId, Principals.getCurrentPrincipals() );
    }

    @Timed
    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + ROLE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppEntitySetPermissionsConfig(
            @PathVariable( ID ) UUID appId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody Map<Permission, Map<UUID, Optional<Set<UUID>>>> permissions ) {
        ensureAdminAccess();
        appService.updateAppRolePermissions( appId, roleId, permissions );
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
            path = UPDATE_PATH + ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateDefaultAppSettings(
            @PathVariable( ID ) UUID appId,
            @RequestBody Map<String, Object> defaultSettings ) {
        ensureAdminAccess();
        appService.updateDefaultAppSettings( appId, defaultSettings );
    }

    @Timed
    @Override
    @RequestMapping(
            path = CONFIG_PATH + UPDATE_PATH + ID_PATH + ORGANIZATION_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppConfigSettings(
            @PathVariable( ID ) UUID appId,
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @RequestBody Map<String, Object> newSettings ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.updateAppConfigSettings( appId, organizationId, newSettings );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
