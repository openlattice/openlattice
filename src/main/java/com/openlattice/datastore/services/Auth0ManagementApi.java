

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

package com.openlattice.datastore.services;

import java.util.Map;
import java.util.Set;

import com.openlattice.directory.pojo.Auth0UserBasic;

import retrofit2.http.*;

// Internal use only! Do NOT add to JDK
public interface Auth0ManagementApi {

    String BASIC_REQUEST_FIELDS = "?fields=user_id%2Cemail%2Cnickname%2Capp_metadata";

    String PAGE = "page";
    String PER_PAGE = "per_page";
    String QUERY = "q";
    String USERS = "users";

    String USER_ID      = "userId";
    String USER_ID_PATH = "{" + USER_ID + "}";

    @GET( USERS + BASIC_REQUEST_FIELDS )
    Set<Auth0UserBasic> getAllUsers( @Query( PAGE ) int page , @Query( PER_PAGE ) int perPage ) ;

    @GET( USERS + "/" + USER_ID_PATH + BASIC_REQUEST_FIELDS )
    Auth0UserBasic getUser( @Path( USER_ID ) String userId );

    @PATCH( USERS + "/" + USER_ID_PATH )
    Void resetRolesOfUser( @Path( USER_ID ) String userId, @Body Map<String,Object> app_metadata );

    @GET( USERS )
    Set<Auth0UserBasic> searchAllUsers(
            @Query( QUERY ) String searchQuery,
            @Query( PAGE ) int page,
            @Query( PER_PAGE ) int perPage );

}
