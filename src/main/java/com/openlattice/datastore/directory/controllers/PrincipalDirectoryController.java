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

import com.auth0.client.auth.AuthAPI;
import com.auth0.client.mgmt.ManagementAPI;
import com.auth0.exception.Auth0Exception;
import com.auth0.json.mgmt.users.User;
import com.codahale.metrics.annotation.Timed;
import com.google.common.eventbus.EventBus;
import com.openlattice.assembler.PostgresRoles;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.directory.MaterializedViewAccount;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.directory.pojo.DirectedAclKeys;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.users.Auth0SyncService;
import com.openlattice.users.Auth0UtilsKt;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
    @Inject
    private ManagementAPI           managementApi;
    @Inject
    private Auth0SyncService        syncService;
    @Inject
    private EventBus                eventBus;

    @Timed
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

    @Timed
    @Override
    @RequestMapping(
            path = USERS,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, User> getAllUsers() {
        return userDirectoryService.getAllUsers();
    }

    @Timed
    @Override
    @RequestMapping(
            path = { ROLES + CURRENT },
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<SecurablePrincipal> getCurrentRoles() {
        return Principals.getCurrentPrincipals()
                .stream()
                .filter( principal -> principal.getType().equals( PrincipalType.ROLE ) )
                .map( spm::lookup )
                .filter( Objects::nonNull )
                .map( aclKey -> spm.getSecurablePrincipal( aclKey ) )
                .collect( Collectors.toSet() );
    }

    @Timed
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

    @Timed
    @Override
    @RequestMapping(
            path = USERS + USER_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public User getUser( @PathVariable( USER_ID ) String userId ) {
        ensureAdminAccess();
        return userDirectoryService.getUser( userId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = SYNC,
            method = RequestMethod.GET )
    public Void syncCallingUser() {
        /*
         * Important note: getCurrentUser() reads the principal id directly from auth token.
         *
         * This is safe since token has been validated and has an auth0 assigned unique id.
         *
         * It is very important that this is the *first* call for a new user. W
         */
        Principal principal = checkNotNull( Principals.getCurrentUser() );

        try {
            final var user = Auth0UtilsKt.getUser( managementApi, principal.getId() );
            syncService.syncUser( user );
        } catch ( IllegalArgumentException | Auth0Exception e ) {
            throw new BadCredentialsException( "Unable to retrieve user profile information from auth0", e );
        }
        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = DB,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public MaterializedViewAccount getMaterializedViewAccount() {
        final var principal = PostgresRoles.buildPostgresUsername( Principals.getCurrentSecurablePrincipal() );
        return new MaterializedViewAccount( principal, dbCredService.getDbCredential( principal ) );
    }

    @Timed
    @Override
    @GetMapping(
            path = USERS + SEARCH + SEARCH_QUERY_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, Auth0UserBasic> searchAllUsers( @PathVariable( SEARCH_QUERY ) String searchQuery ) {
        String wildcardSearchQuery = searchQuery + "*";
        return userDirectoryService.searchAllUsers( wildcardSearchQuery );
    }

    @Timed
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

    @Timed
    @Override
    @PostMapping(
            path = UPDATE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void addPrincipalToPrincipal( @RequestBody DirectedAclKeys directedAclKeys ) {
        ensureWriteAccess( directedAclKeys.getTarget() );
        ensureOwnerAccess( directedAclKeys.getSource() );

        spm.addPrincipalToPrincipal( directedAclKeys.getSource(), directedAclKeys.getTarget() );

        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            path = UPDATE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void removePrincipalFromPrincipal( @RequestBody DirectedAclKeys directedAclKeys ) {
        ensureWriteAccess( directedAclKeys.getTarget() );
        ensureOwnerAccess( directedAclKeys.getSource() );

        spm.removePrincipalFromPrincipal( directedAclKeys.getSource(), directedAclKeys.getTarget() );

        return null;
    }

    @Override
    @DeleteMapping( path = USERS + USER_ID_PATH )
    public Void deleteUserAccount( @PathVariable( USER_ID ) String userId ) {
        ensureAdminAccess();

        SecurablePrincipal securablePrincipal = spm.getPrincipal( userId );
        spm.deletePrincipal( securablePrincipal.getAclKey() );

        userDirectoryService.deleteUser( userId );

        return null;
    }
}
