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

package com.openlattice.datastore.directory.controllers;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.auth0.client.auth.AuthAPI;
import com.auth0.exception.Auth0Exception;
import com.auth0.json.auth.UserInfo;
import com.google.common.collect.ImmutableSet;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.bootstrap.AuthorizationBootstrap;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.roles.SecurePrincipalsManager;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( PrincipalApi.CONTROLLER )
public class PrincipalDirectoryController implements PrincipalApi, AuthorizingComponent {

    @Inject
    private DbCredentialService     dbCredService;
    @Inject
    private UserDirectoryService    userDirectoryService;
    @Inject
    private SecurePrincipalsManager spm;
    @Inject
    private AuthorizationManager    authorizations;
    @Inject
    private AuthAPI                 authApi;

    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public SecurablePrincipal getSecurablePrincipal( @RequestBody Principal principal ) {
        AclKey aclKey = spm.lookup( principal );

        if ( !principal.getType().equals( PrincipalType.USER ) ) {
            ensureReadAccess( aclKey );
        }

        return spm.getSecurablePrincipal( aclKey );
    }

    @Override
    @RequestMapping(
            path = USERS,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, Auth0UserBasic> getAllUsers() {
        return userDirectoryService.getAllUsers();
    }

    @Override
    @RequestMapping(
            path = { ROLES + CURRENT },
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<SecurablePrincipal> getCurrentRoles() {
        return Principals.getCurrentPrincipals()
                .stream()
                .filter( principal -> principal.getType().equals( PrincipalType.ROLE ) )
                .map( principal -> spm.lookup( principal ))
                .filter( aclKey -> aclKey != null )
                .map( aclKey -> spm.getSecurablePrincipal( aclKey ) )
                .collect( Collectors.toSet() );
    }

    @Override
    @RequestMapping(
            path = ROLES,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<AclKey, Role> getAvailableRoles() {
        return authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.Role,
                EnumSet.of( Permission.READ ) )
                .map( AclKey::new )
                .collect( Collectors
                        .toMap( Function.identity(), aclKey -> (Role) spm.getSecurablePrincipal( aclKey ) ) );
    }

    @Override
    @RequestMapping(
            path = USERS + USER_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Auth0UserBasic getUser( @PathVariable( USER_ID ) String userId ) {
        return userDirectoryService.getUser( userId );
    }

    @Override
    @RequestMapping(
            path = USERS,
            method = RequestMethod.POST,
            consumes = MediaType.TEXT_PLAIN_VALUE )
    public Collection<SecurablePrincipal> activateUser( @RequestBody String accessToken ) {
        Principal principal = checkNotNull( Principals.getCurrentUser() );

        UserInfo userInfo;

        Map<String, Object> values;
        String userId;
        String tokenUserId;

        try {
            userInfo = checkNotNull( authApi.userInfo( accessToken ).execute() );
            values = userInfo.getValues();
            userId = principal.getId();
            tokenUserId = (String) values.get( "sub" );
            checkState( StringUtils.equals( userId, tokenUserId ),
                    "User %s in header does not match user %s retrieved by access token.",
                    userId,
                    tokenUserId );
        } catch ( IllegalArgumentException | Auth0Exception e ) {
            throw new BadCredentialsException( "Unable to retrieve user profile information from auth0", e );
        }

        //Auth0UserBasic user = spm.getUser( principal.getId() );
        if ( !spm.principalExists( principal ) ) {
            //TODO: Store more useful information in Auth0 about the user
            //We create securable principal first as db creds can be reset separately
            String title = (String) values.get( "name" );

            if ( StringUtils.isBlank( title ) ) {
                title = (String) values.get( "nickname" );
            }

            if ( StringUtils.isBlank( title ) ) {
                title = (String) values.get( "email" );
            }

            if ( StringUtils.isBlank( title ) ) {
                title = tokenUserId;
            }

            spm.createSecurablePrincipalIfNotExists( principal,
                    new SecurablePrincipal( Optional.empty(), principal, title, Optional.empty() ) );

            dbCredService.createUserIfNotExists( userId );
        }

        AclKey userAclKey = spm.lookup( principal );

        AclKey userRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_USER_ROLE.getPrincipal() );
        AclKey adminRoleAclKey = spm.lookup( AuthorizationBootstrap.GLOBAL_ADMIN_ROLE.getPrincipal() );
        Auth0UserBasic user = userDirectoryService.getUser( userId );

        if ( user.getRoles().contains( SystemRole.AUTHENTICATED_USER.getName() ) ) {
            spm.addPrincipalToPrincipal( userRoleAclKey, userAclKey );
        }

        if ( user.getRoles().contains( SystemRole.ADMIN.getName() ) ) {
            spm.addPrincipalToPrincipal( adminRoleAclKey, userAclKey );
        }

        return spm.getAllPrincipals( spm.getSecurablePrincipal( userAclKey ) );
    }

    @Override
    @RequestMapping(
            path = DB,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public String getDbAccessCredential() {
        final var principal = Principals.getCurrentUser();
        return dbCredService.getDbCredential( principal.getId() );
    }

    @Override
    @GetMapping(
            path = USERS + SEARCH + SEARCH_QUERY_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, Auth0UserBasic> searchAllUsers( @PathVariable( SEARCH_QUERY ) String searchQuery ) {

        String wildcardSearchQuery = searchQuery + "*";
        return userDirectoryService.searchAllUsers( wildcardSearchQuery );
    }

    @Override
    @GetMapping(
            path = USERS + SEARCH_EMAIL + EMAIL_SEARCH_QUERY_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, Auth0UserBasic> searchAllUsersByEmail( @PathVariable( SEARCH_QUERY ) String emailSearchQuery ) {

        // to search by an exact email, the search query must be in this format: email.raw:"hristo@openlattice.com"
        // https://auth0.com/docs/api/management/v2/user-search#search-by-email
        String exactEmailSearchQuery = "email.raw:\"" + emailSearchQuery + "\"";

        return userDirectoryService.searchAllUsers( exactEmailSearchQuery );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }
}
