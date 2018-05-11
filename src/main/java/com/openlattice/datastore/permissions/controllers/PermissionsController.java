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

package com.openlattice.datastore.permissions.controllers;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.openlattice.authorization.Ace;
import com.openlattice.authorization.AceExplanation;
import com.openlattice.authorization.Acl;
import com.openlattice.authorization.AclData;
import com.openlattice.authorization.AclExplanation;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.ForbiddenException;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.PermissionsApi;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.datastore.exceptions.BadRequestException;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( PermissionsApi.CONTROLLER )
public class PermissionsController implements PermissionsApi, AuthorizingComponent {
    private static final Logger logger = LoggerFactory.getLogger( PermissionsController.class );

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private SecurePrincipalsManager securePrincipalsManager;

    @Inject
    private EventBus eventBus;

    @Override
    @RequestMapping(
            path = { "", "/" },
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateAcl( @RequestBody AclData req ) {
        /*
         * Ensure that the user has alter permissions on Acl permissions being modified
         */
        final Acl acl = req.getAcl();
        final AclKey aclKeys = new AclKey( acl.getAclKey() );
        if ( isAuthorized( Permission.OWNER ).test( aclKeys ) ) {
            switch ( req.getAction() ) {
                case ADD:
                    acl.getAces().forEach(
                            ace -> authorizations.addPermission(
                                    aclKeys,
                                    ace.getPrincipal(),
                                    ace.getPermissions() ) );
                    break;
                case REMOVE:
                    acl.getAces().forEach(
                            ace -> authorizations.removePermission(
                                    aclKeys,
                                    ace.getPrincipal(),
                                    ace.getPermissions() ) );
                    break;
                case SET:
                    acl.getAces().forEach(
                            ace -> authorizations.setPermission(
                                    aclKeys,
                                    ace.getPrincipal(),
                                    ace.getPermissions() ) );
                    break;
                default:
                    logger.error( "Invalid action {} specified for request.", req.getAction() );
                    throw new BadRequestException( "Invalid action specified: " + req.getAction() );
            }
        } else {
            throw new ForbiddenException( "Only owner of a securable object can access other users' access rights." );
        }
        return null;
    }

    @Override
    @RequestMapping(
            path = { "", "/" },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Acl getAcl( @RequestBody AclKey aclKeys ) {
        if ( isAuthorized( Permission.OWNER ).test( aclKeys ) ) {
            return authorizations.getAllSecurableObjectPermissions( aclKeys );
        } else {
            throw new ForbiddenException( "Only owner of a securable object can access other users' access rights." );
        }
    }

    @Override
    @RequestMapping(
            path = { EXPLAIN },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AclExplanation getAclExplanation( @RequestBody AclKey aclKey ) {
        if ( isAuthorized( Permission.OWNER ).test( aclKey ) ) {
            SetMultimap<Principal, Ace> resultMap = HashMultimap.create();

            Iterable<Ace> aces = authorizations.getAllSecurableObjectPermissions( aclKey ).getAces();
            for ( Ace ace : aces ) {
                Principal principal = ace.getPrincipal();
                resultMap.put( principal, ace );
                if ( principal.getType() == PrincipalType.ROLE ) {
                    // add inherited permissions of users from the role
                    Iterable<Principal> users = securePrincipalsManager
                            .getAllUsersWithPrincipal( securePrincipalsManager.lookup( principal ) );

                    for ( Principal user : users ) {
                        resultMap.put( user, ace );
                    }
                }
            }

            // compute total permission of each user
            Iterable<AceExplanation> explanation = Iterables.transform( resultMap.asMap().entrySet(),
                    this::computeAceExplanation );
            return new AclExplanation( aclKey, explanation::iterator );
        } else {
            throw new ForbiddenException( "Only owner of a securable object can access other users' access rights." );
        }
    }

    /**
     * Compute the total permission of a user has from his aces, thus computing the Ace explanation
     */
    private AceExplanation computeAceExplanation( Entry<Principal, Collection<Ace>> entry ) {
        Set<Permission> totalPermissions = entry.getValue().stream().flatMap( ace -> ace.getPermissions().stream() )
                .collect( Collectors.toCollection( () -> EnumSet.noneOf( Permission.class ) ) );
        Set<Ace> aces = entry.getValue().stream().collect( Collectors.toSet() );
        Ace totalAce = new Ace( entry.getKey(), totalPermissions );
        return new AceExplanation( totalAce, aces );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }
}
