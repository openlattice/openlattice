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

import java.util.List;
import java.util.UUID;

import retrofit2.http.Body;
import retrofit2.http.POST;
import retrofit2.http.PUT;

@Deprecated
public interface PermissionsRequestsApi {
    /*
     * These determine the service routing for the LB
     */
    String SERVICE    = "/datastore";
    String CONTROLLER = "/permissionsrequests";
    String BASE       = SERVICE + CONTROLLER;

    /*
     * These are the actual components after {SERVICE}/{CONTROLLER}/
     */
    String ADMIN       = "admin";
    String UNRESOLVED  = "unresolved";
    String RESOLVED    = "resolved";

    /**
     * Update/Insert a Permission Request. For one user and one securable object List &lt;UUID &gt; , there should only be one unresolved request at any given time. 
     * @param req 
     * <ul>
     *   <li>aclRoot is the root of your permissions request. 
     *     <ul>
     *       <li>If you are requesting a nested object, the root is the truncated List &lt; UUID &gt; without the last element.</li>
     *       <li>If you are requesting a standalone object, the root is the List &lt; UUID &gt; itself.</li>
     *     </ul>
     *   </li>
     *   <li>permissions is a map that specifies the children that you are requesting access to, as well as the permissions you are requesting.
     *     <ul>
     *       <li>If you are requesting a nested object, a child is the last element in the full List &lt; UUID &gt;. The permissions field is then a Map &lt; Child, Set &lt; Permission &gt;&gt;</li>
     *       <li>If you are requesting a standalone object, a child is null. In this case, <b>one should put the last UUID of aclRoot as the key of the permissions map.</b> </li>
     *     </ul>
     *   </li>
     * </ul>
     * @return
     */
    @PUT( BASE )
    Void upsertRequest( @Body AclRootRequestDetailsPair req );

    /**
     * Get the unresolved request for the current user and the specified securable object. If no such request exist, a 404 should be returned.
     * @param aclRoot
     * @return
     */
    @POST( BASE + "/" + UNRESOLVED )
    PermissionsRequest getUnresolvedRequestOfUser( @Body List<UUID> aclRoot );

    /**
     * Get all resolved requests for the current user and the specified securable object.
     * @param aclRoot
     * @return
     */
    @POST( BASE + "/" + RESOLVED )
    Iterable<PermissionsRequest> getResolvedRequestsOfUser( @Body List<UUID> aclRoot );

    /**
     * Allow owner of a securable object to change the status of an UNRESOLVED permissions request. This allows them to approve/decline a request.
     * @param req Only aclRoot, user, and status has to be passed in.
     * @return
     */
    @POST( BASE + "/" + ADMIN )
    Void updateUnresolvedRequestStatus( @Body PermissionsRequest req );

    /**
     * Allow owner of a securable object to retrieve all unresolved requests.
     * @param req Both aclRoot and status can be missing. If aclRoot is missing, all authorized objects of user would be fetched. If status is missing, all RequestStatus would be fetched.
     * @return
     */
    @POST( BASE + "/" + ADMIN + "/" + UNRESOLVED )
    Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin( @Body AclRootStatusPair req );

}
