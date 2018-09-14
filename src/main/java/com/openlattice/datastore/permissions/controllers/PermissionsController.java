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

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.openlattice.authorization.*;
import com.openlattice.datastore.exceptions.BadRequestException;
import com.openlattice.organizations.roles.SecurePrincipalsManager;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.Map.Entry;
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
                                    ace.getPermissions(),
                                    ace.getExpirationDate() ) );
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
                                    ace.getPermissions(),
                                    ace.getExpirationDate() ) );
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

    /*
    @Override
    @RequestMapping(
            path = { EXPLAIN },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AclExplanation getAclExplanation( @RequestBody AclKey aclKey ) {
        //needs to return a map of principles to the path that grants permission with expiration
        if ( isAuthorized( Permission.OWNER ).test( aclKey ) ) {
            SetMultimap<Principal, Ace> resultMap = HashMultimap
                    .create(); //will hold all aces that contain the key principal

            Iterable<Ace> aces = authorizations.getAllSecurableObjectPermissions( aclKey )
                    .getAces(); //gets aces from returned acl
            Map<PrincipalType, List<Ace>> acesByType = StreamUtil.stream( aces )
                    .collect( Collectors.groupingBy( ace -> ace.getPrincipal().getType() ) );
            //for principals that aren't users, sPM.getallPrincipalsbyPrincipal
            Map<Principal, Set<Principal>>
            for ( Ace ace : aces ) {
                //build all paths for how a person has access
                Principal principal = ace.getPrincipal();
                resultMap.put( principal, ace );
                if ( principal.getType() == PrincipalType.ROLE ) {
                    // add inherited permissions of users from the role
                    Iterable<Principal> users = securePrincipalsManager
                            .getAllUsersWithPrincipal( securePrincipalsManager.lookup( principal ) );

                    for ( Principal user : users ) { //can add a new ace to a principal that is already in the multimap
                        resultMap.put( user, ace );
                    }
                }
            }

            // compute total permission of each user
            Iterable<AceExplanation> explanation = Iterables.transform( resultMap.asMap().entrySet(),
                    this::computeAceExplanation ); // passes entry in as key to collection of all aces
            return new AclExplanation( aclKey, explanation::iterator );
        } else {
            throw new ForbiddenException( "Only owner of a securable object can access other users' access rights." );
        }
    }


    // Compute the total permission of a user has from his aces, thus computing the Ace explanation

    private AceExplanation computeAceExplanation( Entry<Principal, Collection<Ace>> entry ) {
        Set<Permission> totalPermissions = entry.getValue().stream().flatMap( ace -> ace.getPermissions().stream() )
                .collect( Collectors.toCollection( () -> EnumSet.noneOf( Permission.class ) ) );
        //OffsetDateTime expirationDate = entry.getValue().stream().flatMap(ace -> ace.getExpirationDate()).collect(  );
        Set<Ace> aces = entry.getValue().stream().collect( Collectors.toSet() );
        Ace totalAce = new Ace( entry.getKey(), totalPermissions );
        return new AceExplanation( totalAce, aces ); //list of securablePrincipals with expiration date
    }
    */

    @Override
    @RequestMapping(
            path = { EXPLAIN },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<Principal, List<List<Principal>>> getAclExplanation( @RequestBody AclKey aclKey ) {
        //needs to return a map of principles to the path that grants permission with expiration
        ensureOwnerAccess( aclKey );
        SetMultimap<Principal, Ace> resultMap = HashMultimap
                .create(); //will hold all aces that contain the key principal

        Iterable<Ace> aces = authorizations.getAllSecurableObjectPermissions( aclKey )
                .getAces(); //gets aces from returned acl
        Map<PrincipalType, List<Ace>> acesByType = StreamUtil.stream( aces )
                .collect( Collectors.groupingBy( ace -> ace.getPrincipal().getType() ) );
        //for principals that aren't users, sPM.getallPrincipalsbyPrincipal gets all of the parents of those principals
        Map<Principal, List<List<Principal>>> principalToPrincipalPaths = new HashMap<>();
        //for all principals with direct access
        for ( Ace ace : aces ) {
            //build all paths for how a person has access
            Principal principal = ace.getPrincipal();
            //OffsetDateTime expirationDate = ace.getExpirationDate();
            List<List<Principal>> paths = new ArrayList<>();

            principalToPrincipalPaths.put( principal, paths );

            //get parent layer
            Collection<SecurablePrincipal> parentLayer = securePrincipalsManager
                    .getParentPrincipalsOfPrincipal( securePrincipalsManager.lookup( principal ) );
            //if parent layer isn't empty, start recursively getting paths
            if ( !parentLayer.isEmpty() ) {
                for ( SecurablePrincipal parent : parentLayer ) {
                    paths = securePrincipalsManager
                            .recursivelyGetPrincipalsPath( parent.getPrincipal(), principalToPrincipalPaths );
                    paths.forEach( path -> path.add(0, principal) );
                    principalToPrincipalPaths.put(principal, paths);
                }
            }
        }
        return principalToPrincipalPaths;

    }

    /*
     //Compute the total permission of a user has from his aces, thus computing the Ace explanation
    private AceExplanation computeAceExplanation( Entry<Principal, Collection<Ace>> entry ) {
        Set<Permission> totalPermissions = entry.getValue().stream().flatMap( ace -> ace.getPermissions().stream() )
                .collect( Collectors.toCollection( () -> EnumSet.noneOf( Permission.class ) ) );
        //OffsetDateTime expirationDate = entry.getValue().stream().flatMap(ace -> ace.getExpirationDate()).collect(  );
        Set<Ace> aces = entry.getValue().stream().collect( Collectors.toSet() );
        Ace totalAce = new Ace( entry.getKey(), totalPermissions );
        return new AceExplanation( totalAce, aces ); //list of securablePrincipals with expiration date
    }
    */

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }
}
