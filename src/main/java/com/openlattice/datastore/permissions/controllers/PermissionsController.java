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
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
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

    @Override
    @RequestMapping(
            path = { EXPLAIN },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<Principal, List<List<Principal>>> getAclExplanation( @RequestBody AclKey aclKey ) {
        //needs to return a map of principles to the path that grants permission with expiration
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
                .toMap( ace -> ace.getPrincipal(), ace -> Lists.newArrayList( Lists.newArrayList() ) ) );
        principalToPrincipalPaths.forEach( ( p, pl ) -> {
            List<Principal> path = Arrays.asList( p );
            pl.add( path );
        } );

        //maps all principals to principals path
        Map<Principal, List<List<Principal>>> currentMap = new HashMap<>( principalToPrincipalPaths ); //creates copy of leaf principals map
        Set<Principal> currentLayer = currentMap.keySet();  //sets keys of current map (leaves of tree) as current layer
        while ( !currentLayer.isEmpty() ) { //while we have nodes to get paths for
            Map<Principal, List<List<Principal>>> parentMap = new HashMap<>(); //keys in this map will be all parents of nodes in current layer
            for ( Entry<Principal, List<List<Principal>>> e : currentMap.entrySet() ) {
                //get parent layer of this node in current layer
                Collection<SecurablePrincipal> parentLayer = securePrincipalsManager
                        .getParentPrincipalsOfPrincipal( securePrincipalsManager.lookup( e.getKey() ) );
                for ( SecurablePrincipal parent : parentLayer ) {
                    Principal parentAsPrincipal = parent.getPrincipal();
                    //add parent principal to each list in child's path
                    List<List<Principal>> paths = e.getValue().stream().map( pl -> {
                        pl.add( parentAsPrincipal );
                        return pl;
                    } ).collect( Collectors.toList() );
                    parentMap.merge( parentAsPrincipal, paths, ( p1, p2 ) -> {
                        p1.addAll( p2 );
                        return p1;
                    } );
                    //add paths to principalToPrincipalPaths map
                    principalToPrincipalPaths.merge( parentAsPrincipal, paths, ( p1, p2 ) -> {
                        p1.addAll( p2 );
                        return p1;
                    } );
                }
            }
            //parent layer becomes current layer
            currentLayer = parentMap.keySet();
            currentMap = parentMap;
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
