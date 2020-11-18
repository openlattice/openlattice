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

package com.openlattice.authorization;

import java.util.Set;

import com.openlattice.authorization.paging.AuthorizedObjectsSearchResult;
import com.openlattice.authorization.securable.SecurableObjectType;

import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Query;

public interface AuthorizationsApi {
    /*
     * These determine the service routing for the LB
     */
    String SERVICE      = "/datastore";
    String CONTROLLER   = "/authorizations";
    String BASE         = SERVICE + CONTROLLER;

    String OBJECT_TYPE  = "objectType";
    String PERMISSION   = "permission";
    String PAGING_TOKEN = "pagingToken";

    @POST( BASE )
    Iterable<Authorization> checkAuthorizations( @Body Set<AccessCheck> queries );

    /**
     * Returns paged results for all authorized objects of specified objectType, that the current user has specified permission for. 
     * @param objectType Required field. Specifying the Securable Object Type that user wants to search for.
     * @param permission Required field. Specifying the permission the user must have for the accessible objects.
     * @param pagingToken Unrequired field. One may use the paging token from previous search result to get to the next page of results.
     * @return
     */
    @GET( BASE )
    AuthorizedObjectsSearchResult getAccessibleObjects(
            @Query( OBJECT_TYPE ) SecurableObjectType objectType,
            @Query( PERMISSION ) Permission permission,
            @Query( PAGING_TOKEN ) String pagingToken );

}
