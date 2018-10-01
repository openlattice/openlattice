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
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.openlattice.authorization.*;
import com.openlattice.datastore.exceptions.BadRequestException;
import com.openlattice.organizations.roles.SecurePrincipalsManager;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
                            ace -> {
                                if ( ace.getExpirationDate().equals( OffsetDateTime.MAX ) ) {
                                    authorizations.addPermission(
                                            aclKeys,
                                            ace.getPrincipal(),
                                            ace.getPermissions() );
                                } else {
                                    authorizations.addPermission(
                                            aclKeys,
                                            ace.getPrincipal(),
                                            ace.getPermissions(),
                                            ace.getExpirationDate() );
                                }
                            } );
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
                            ace -> {
                                if ( ace.getExpirationDate().equals( OffsetDateTime.MAX ) ) {
                                    authorizations.setPermission(
                                            aclKeys,
                                            ace.getPrincipal(),
                                            ace.getPermissions() );
                                } else {
                                    authorizations.setPermission(
                                            aclKeys,
                                            ace.getPrincipal(),
                                            ace.getPermissions(),
                                            ace.getExpirationDate() );
                                }
                            } );
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
    public Collection<AclExplanation> getAclExplanation( @RequestBody AclKey aclKey ) {
        ensureOwnerAccess( aclKey );

        //maps aces to principal type
        Iterable<Ace> aces = authorizations.getAllSecurableObjectPermissions( aclKey )
                .getAces(); //gets aces from returned acl
        Map<PrincipalType, List<Ace>> acesByType = StreamUtil.stream( aces )
                .collect( Collectors.groupingBy( ace -> ace.getPrincipal().getType() ) );

        //maps non-user principals to a List<List<Principal>> containing one list of themselves
        Stream<Ace> aceStream = acesByType.entrySet().stream()
                .filter( e -> e.getKey() != PrincipalType.USER )
                .flatMap( e -> e.getValue().stream() );
        Map<Principal, List<List<Principal>>> principalToPrincipalPaths = aceStream.collect( Collectors
                .toMap( Ace::getPrincipal, ace -> Lists.newArrayList( Lists.newArrayList() ) ) );
        principalToPrincipalPaths.forEach( ( p, pl ) -> {
            List<Principal> path = new ArrayList<Principal>( Arrays.asList( p ) );
            pl.add( path );
        } );

        //maps all principals to principals path that grant permission on the acl key
        Set<Principal> currentLayer = new HashSet<>( principalToPrincipalPaths.keySet() );
        while ( !currentLayer.isEmpty() ) { //while we have nodes to get paths for
            Set<Principal> parentLayer = new HashSet<>();
            for ( Principal p : currentLayer ) {
                List<List<Principal>> child_paths = principalToPrincipalPaths.get( p );
                Set<Principal> currentParents = securePrincipalsManager
                        .getParentPrincipalsOfPrincipal( securePrincipalsManager.lookup( p ) ).stream()
                        .map( SecurablePrincipal::getPrincipal )
                        .collect( Collectors.toSet() );
                if ( currentParents.contains( p ) ) { currentParents.remove( p ); } //removes self-loops
                for ( Principal parent : currentParents ) {
                    List<List<Principal>> paths = principalToPrincipalPaths
                            .getOrDefault( parent, new ArrayList<>() );
                    //if map doesn't contain entry for parent, add it to map with current empty paths object
                    if ( paths.isEmpty() ) {
                        principalToPrincipalPaths.put( parent, paths );
                    }
                    //build paths
                    for ( List<Principal> path : child_paths ) {
                        var new_path = new ArrayList( path );
                        new_path.add( parent );
                        if ( !paths.contains( new_path ) ) { paths.add( new_path ); }
                    }
                }
                parentLayer.addAll( currentParents );
            }
            currentLayer = parentLayer;
        }
        
        //collect map entries as aclExplanations
        Collection<AclExplanation> aclExplanations = principalToPrincipalPaths.entrySet().stream().map( entry -> {
            AclExplanation aclExp = new AclExplanation( entry.getKey(), entry.getValue() );
            return aclExp;
        } ).collect( Collectors.toSet() );
        return aclExplanations;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }
}
