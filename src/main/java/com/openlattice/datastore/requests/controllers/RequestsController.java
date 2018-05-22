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

package com.openlattice.datastore.requests.controllers;

import com.google.common.base.Predicates;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.Principals;
import com.openlattice.requests.HazelcastRequestsManager;
import com.openlattice.requests.Request;
import com.openlattice.requests.RequestStatus;
import com.openlattice.requests.RequestsApi;
import com.openlattice.requests.Status;
import com.openlattice.requests.util.RequestUtil;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( RequestsApi.CONTROLLER )
public class RequestsController implements RequestsApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private HazelcastRequestsManager hrm;

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    @Override
    @GetMapping(
            path = { "", "/" } )
    public Iterable<Status> getMyRequests() {
        return hrm.getStatuses( Principals.getCurrentUser() )::iterator;
    }

    @Override
    @GetMapping(
            path = REQUEST_STATUS_PATH )
    public Iterable<Status> getMyRequests( @PathVariable( REQUEST_STATUS ) RequestStatus requestStatus ) {
        return hrm.getStatuses( Principals.getCurrentUser(), requestStatus )::iterator;
    }

    @Override
    @PutMapping(
            path = { "", "/" } )
    public Void submit( @RequestBody Set<Request> requests ) {
        Map<AceKey, Status> statusMap = RequestUtil.reqsAsStatusMap( requests );
        hrm.submitAll( statusMap );
        return null;
    }

    @Override
    @PostMapping(
            path = { "", "/" },
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Status> getStatuses( @RequestBody Set<AclKey> aclKeys ) {
        return aclKeys.stream().flatMap( this::getStatuses )::iterator;
    }

    @Override
    @PostMapping(
            path = REQUEST_STATUS_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Status> getStatuses(
            @PathVariable( REQUEST_STATUS ) RequestStatus requestStatus,
            @RequestBody Set<AclKey> aclKeys ) {
        return aclKeys.stream().flatMap( getStatusesInStatus( requestStatus ) )::iterator;
    }

    @Override
    @PatchMapping(
            path = { "", "/" },
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void updateStatuses( @RequestBody Set<Status> statuses ) {
        if ( statuses.stream().map( Status::getRequest ).map( Request::getAclKey ).allMatch( this::owns ) ) {
            Map<AceKey, Status> statusMap = RequestUtil.statusMap( statuses );
            hrm.submitAll( statusMap );
            return null;
        }
        throw new AccessDeniedException("Not authorized to update statuses.");
    }

    private Function<AclKey, Stream<Status>> getStatusesInStatus( RequestStatus requestStatus ) {
        return aclKey -> owns( aclKey ) ? hrm.getStatusesForAllUser( aclKey, requestStatus )
                : hrm.getStatuses( Stream.of( new AceKey( aclKey, Principals.getCurrentUser() ) ) )
                        .filter( Predicates.notNull()::apply )
                        .filter( status -> status.getStatus().equals( requestStatus ) );
    }

    private Stream<Status> getStatuses( AclKey aclKey ) {
        return owns( aclKey ) ? hrm.getStatusesForAllUser( aclKey )
                : hrm.getStatuses( Stream.of( new AceKey( aclKey, Principals.getCurrentUser() ) ) );
    }
}
