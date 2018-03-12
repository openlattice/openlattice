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

package com.openlattice.directory;

import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.organization.roles.Role;
import com.openlattice.authorization.AclKey;
import java.util.Collection;
import java.util.Map;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

public interface PrincipalApi {
    /*
     * These determine the service routing for the LB
     */
    String SERVICE    = "/datastore";
    String CONTROLLER = "/principals";
    String BASE       = SERVICE + CONTROLLER;

    /*
     * Path variables
     */
    String USER_ID      = "userId";
    String ROLE         = "role";
    String SEARCH_QUERY = "searchQuery";

    /*
     * Fixed paths
     */
    String EMAIL  = "/email";
    String ROLES  = "/roles";
    String SEARCH = "/search";
    String USERS  = "/users";
    String DB     = "/db";

    String SEARCH_EMAIL = SEARCH + EMAIL;

    /*
     * Variable paths
     */

    String USER_ID_PATH            = "/{" + USER_ID + "}";
    String ROLE_PATH               = "/{" + ROLE + "}";
    String SEARCH_QUERY_PATH       = "/{" + SEARCH_QUERY + "}";
    String EMAIL_SEARCH_QUERY_PATH = "/{" + SEARCH_QUERY + ":.+" + "}";

    @GET( BASE + USERS )
    Map<String, Auth0UserBasic> getAllUsers();

    @GET( BASE + ROLES )
    Map<AclKey, Role> getAvailableRoles();

    @GET( BASE + USERS + USER_ID_PATH )
    Auth0UserBasic getUser( @Path( USER_ID ) String userId );

    @GET( BASE + DB + USER_ID_PATH )
    String getDbAccessCredential( @Path( USER_ID ) String userId );

    @GET( BASE + USERS + SEARCH + SEARCH_QUERY_PATH )
    Map<String, Auth0UserBasic> searchAllUsers( @Path( SEARCH_QUERY ) String searchQuery );

    @GET( BASE + USERS + SEARCH_EMAIL + EMAIL_SEARCH_QUERY_PATH )
    Map<String, Auth0UserBasic> searchAllUsersByEmail( @Path( SEARCH_QUERY ) String emailSearchQuery );

    /**
     * Activates a user in the OpenLattice system. This call must be made once before a user will be available for use
     * in authorization policies.
     *
     * @param accessToken An access token that can be used to retrieve the user profile.
     * @return Nothing
     */
    @POST( BASE + USERS )
    Collection<SecurablePrincipal> activateUser( @Body String accessToken );
}
