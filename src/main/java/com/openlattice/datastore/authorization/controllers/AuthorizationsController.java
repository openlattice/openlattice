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

package com.openlattice.datastore.authorization.controllers;

import com.codahale.metrics.annotation.Timed;
import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.Authorization;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizationsApi;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.paging.AuthorizedObjectsSearchResult;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.google.common.collect.Maps;
import com.openlattice.authorization.AclKey;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( AuthorizationsApi.CONTROLLER )
public class AuthorizationsController implements AuthorizationsApi, AuthorizingComponent {
    private static final Logger logger            = LoggerFactory.getLogger( AuthorizationsController.class );
    //Number of authorized objects in each page of results
    private static final int    DEFAULT_PAGE_SIZE = 10;

    @Inject
    private AuthorizationManager authorizations;

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Authorization> checkAuthorizations( @RequestBody Set<AccessCheck> queries ) {
        //        Map<AclKey, EnumMap<Permission, Boolean>> accessChecks = new HashMap<>( queries.size() );
        //
        //        for ( AccessCheck accessCheck : queries ) {
        //            EnumMap<Permission, Boolean> results = new EnumMap<>( Permission.class );
        //            accessCheck.getPermissions().forEach( p -> results.put( p, false ) );
        //            accessChecks.put( accessCheck.getAclKey(), results );
        //        }
        //
        //        Map<AceKey, AceValue> permissionMap =
        //                authorizations.getPermissionMap( accessChecks.keySet(), Principals.getCurrentPrincipals() );
        //
        //        permissionMap.forEach( ( k, v ) -> {
        //            //The permission map will have null ace values for missing aces.
        //            if ( v != null ) {
        //                EnumMap<Permission, Boolean> results = accessChecks.get( k.getAclKey() );
        //                checkNotNull( results, "Got a permission back for an acl key that wasn't requested" );
        //                EnumSet<Permission> permissions = v.getPermissions();
        //
        //                for ( Permission p : results.keySet() ) {
        //                    if ( permissions.contains( p ) ) {
        //                        results.put( p, true );
        //                    }
        //                }
        //            }
        //        } );

        Iterable<Authorization> methodA = authorizations
                .accessChecksForPrincipals( queries, Principals.getCurrentPrincipals() )::iterator;
//        Iterable<Authorization> methodB = authorizations
//                .maybeFastAccessChecksForPrincipals( queries, Principals.getCurrentPrincipals() )
//                .entrySet()
//                .stream()
//                .map( e -> new Authorization( e.getAclKey(), e.getValue() ) )::iterator;
        return methodA;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private Authorization getAuth( AccessCheck query ) {
        Set<Permission> currentPermissions = authorizations
                .getSecurableObjectPermissions( new AclKey( query.getAclKey() ), Principals.getCurrentPrincipals() );
        Map<Permission, Boolean> permissionsMap = Maps.asMap( query.getPermissions(), currentPermissions::contains );
        return new Authorization( query.getAclKey(), permissionsMap );
    }

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AuthorizedObjectsSearchResult getAccessibleObjects(
            @RequestParam(
                    value = OBJECT_TYPE,
                    required = true ) SecurableObjectType objectType,
            @RequestParam(
                    value = PERMISSION,
                    required = true ) Permission permission,
            @RequestParam(
                    value = PAGING_TOKEN,
                    required = false ) String pagingToken
    ) {
        return authorizations.getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                objectType,
                permission,
                pagingToken,
                DEFAULT_PAGE_SIZE );
    }

}
