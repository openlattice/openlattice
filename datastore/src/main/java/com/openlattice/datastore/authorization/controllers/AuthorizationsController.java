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
import com.openlattice.authorization.*;
import com.openlattice.authorization.paging.AuthorizedObjectsSearchResult;
import com.openlattice.authorization.securable.SecurableObjectType;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping( AuthorizationsApi.CONTROLLER )
public class AuthorizationsController implements AuthorizationsApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authorizations;

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Authorization> checkAuthorizations( @RequestBody Set<AccessCheck> queries ) {
        return authorizations.accessChecksForPrincipals( queries, Principals.getCurrentPrincipals() )::iterator;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AuthorizedObjectsSearchResult getAccessibleObjects(
            @RequestParam( value = OBJECT_TYPE ) SecurableObjectType objectType,
            @RequestParam( value = PERMISSION ) Permission permission,
            @RequestParam( value = PAGING_TOKEN, required = false ) String pagingToken
    ) {

        Set<AclKey> authorizedAclKeys = authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                objectType,
                EnumSet.of( permission )
        ).collect( Collectors.toSet() );

        return new AuthorizedObjectsSearchResult( null, authorizedAclKeys );
    }

}
