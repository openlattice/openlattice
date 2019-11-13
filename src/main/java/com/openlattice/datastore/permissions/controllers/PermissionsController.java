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

import com.codahale.metrics.annotation.Timed;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.assembler.PostgresDatabases;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.controllers.exceptions.BadRequestException;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.ExternalDatabaseManagementService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

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

    @Inject
    private ExternalDatabaseManagementService edms;

    @Inject
    private HazelcastSecurableObjectResolveTypeService securableObjectResolveTypeService;

    @Override
    @Timed
    @RequestMapping(
            path = { "", "/" },
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateAcl( @RequestBody AclData req ) {
        return updateAcls( ImmutableList.of( req ) );
    }

    @Override
    @Timed
    @RequestMapping(
            path = { UPDATE },
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateAcls( @RequestBody List<AclData> req ) {

        Map<Action, List<Acl>> requestsByActionType = req.stream().collect( Collectors
                .groupingBy( AclData::getAction, Collectors.mapping( AclData::getAcl, Collectors.toList() ) ) );

        /*
         * Ensure that the user has alter permissions on Acl permissions being modified
         */
        Set<AclKey> unauthorizedAclKeys = authorizations
                .accessChecksForPrincipals( requestsByActionType.entrySet().stream()
                        .filter( entry -> !entry.getKey().equals( Action.REQUEST ) )
                        .flatMap( entry -> entry.getValue().stream() )
                        .map( acl -> new AccessCheck( new AclKey( acl.getAclKey() ), EnumSet.of( Permission.OWNER ) ) )
                        .collect( Collectors.toSet() ), Principals.getCurrentPrincipals() )
                .filter( authorization -> !authorization.getPermissions().get( Permission.OWNER ) )
                .map( Authorization::getAclKey )
                .collect( Collectors.toSet() );

        if ( !unauthorizedAclKeys.isEmpty() ) {
            throw new ForbiddenException( "Only owner of securable objects " + unauthorizedAclKeys +
                    " can access other users' access rights." );
        }

        requestsByActionType.forEach( ( action, acls ) -> {

            switch ( action ) {
                case ADD:
                    authorizations.addPermissions( acls );
                    edms.executePrivilegesUpdate( action, getOrganizationExternalDatabaseAcls( acls ) );
                    break;

                case REMOVE:
                    authorizations.removePermissions( acls );
                    edms.executePrivilegesUpdate( action, getOrganizationExternalDatabaseAcls( acls ) );
                    break;

                case SET:
                    authorizations.setPermissions( acls );
                    edms.executePrivilegesUpdate( action, getOrganizationExternalDatabaseAcls( acls ) );
                    break;

                default:
                    logger.error( "Invalid action {} specified for request.", action );
                    throw new BadRequestException( "Invalid action specified: " + action );
            }
        } );

        return null;
    }

    @Override
    @Timed
    @RequestMapping(
            path = { "", "/" },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Acl getAcl( @RequestBody AclKey aclKeys ) {
        if ( isAuthorized( Permission.OWNER ).test( aclKeys ) ) {
            return authorizations.getAllSecurableObjectPermissions( aclKeys );
        } else {
            throw new ForbiddenException( "Only owner of securable object " + aclKeys +
                    " can access other users' access rights." );
        }
    }

    @Override
    @Timed
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
                        var new_path = new ArrayList<>( path );
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

    private List<Acl> getOrganizationExternalDatabaseAcls( List<Acl> acls ) {
        Set<AclKey> aclKeys = acls.stream().map(acl -> new AclKey( acl.getAclKey() )).collect( Collectors.toSet() );
        Set<AclKey> allOrgExternalDBAclKeys = securableObjectResolveTypeService
                .getOrganizationExternalDatabaseAclKeys( aclKeys );
        return acls.stream().filter( acl -> allOrgExternalDBAclKeys.contains( acl.getAclKey() ) )
                .collect( Collectors.toList() );
    }
}
