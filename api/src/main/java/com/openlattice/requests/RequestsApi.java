/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.requests;

import com.openlattice.authorization.AclKey;
import retrofit2.http.*;

import java.util.Set;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface RequestsApi {
    String SERVICE    = "/datastore";
    String CONTROLLER = "/requests";
    String BASE       = SERVICE + CONTROLLER;

    /*
     * These are the actual components after {SERVICE}/{CONTROLLER}/
     */

    String STATUS              = "/status";
    String REQUEST_STATUS      = "reqStatus";
    String REQUEST_STATUS_PATH = "/{" + REQUEST_STATUS + "}";

    /**
     * Retrieves all permission requests for the current account.
     * <p>
     * The API will stream a potentially very large set of results back to the client
     *
     * @return All permission requests across all objects.
     */
    @GET( BASE )
    Iterable<Status> getMyRequests();

    /**
     * Retrieves all permission requests in a specified state specified by {@code requestStatus} (Example: ({@link RequestStatus#SUBMITTED }, {@link RequestStatus#APPROVED }, {@link RequestStatus#DECLINED }) for the current account.
     *
     * @return All permission requests in a specified state specified by {@code requestStatus}.
     */
    @GET( BASE + REQUEST_STATUS_PATH )
    Iterable<Status> getMyRequests( @Path( REQUEST_STATUS ) RequestStatus requestStatus );

    /**
     * Submits a set of permission requests.
     *
     * @param requests Requests to submit
     * @return
     */
    @PUT( BASE )
    Void submit( @Body Set<Request> requests );

    /**
     * Retrieves all permission requests for a set of AclKeys.
     *
     * @param aclKeys A set of aclKeys for which to retrieve permission requests information.
     * @return For each aclKey ( {@code AclKey} ) this will return all permission requests in any state across all users for
     * callers who are owners and only the caller's permission requests in any state for callers who are not owners.
     */
    @POST( BASE )
    Iterable<Status> getStatuses( @Body Set<AclKey> aclKeys );

    /**
     * Retrieves the permission requests for all objects in the state specified by {@code requestStatus}
     *
     * @param requestStatus The {@link RequestStatus} to match against.
     * @param requests      A list of acl keys whose permission requests will be checked against
     * @return For each aclKey ( {@code AclKey} ) this will return all permission requests in the state specified by {@code requestStatus} across all users for
     * callers who are owners and only the caller's permission requests in the state specified by {@code requestStatus} for callers who are not owners
     */
    @POST( BASE + REQUEST_STATUS_PATH )
    Iterable<Status> getStatuses( @Path( REQUEST_STATUS ) RequestStatus requestStatus, @Body Set<AclKey> requests );

    /**
     * Updates a set of permission requests.
     *
     * @param statuses The permission requests updates to attempt to apply.
     * @return 200 OK if request succeeds or 403 if the user doesn't have ownership over all objects he is attempting to update.
     */
    @PATCH( BASE )
    Void updateStatuses( @Body Set<Status> statuses );

}
