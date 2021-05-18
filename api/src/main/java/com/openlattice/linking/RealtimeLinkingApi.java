/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.linking;

import com.openlattice.data.EntityDataKey;
import java.util.List;
import java.util.Map;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.HTTP;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;

import java.util.Set;
import java.util.UUID;

/**
 * This API is responsible for any real-time linking / matching requests
 * Currently it is only used in rehearsal tests
 */
public interface RealtimeLinkingApi {

    String BLOCKING                   = "/blocking";
    String CONTROLLER                 = "/linking";
    String FINISHED                   = "/finished";
    String LINKING_ENTITY_SET_ID      = "linkingEntitySetId";
    String LINKING_ENTITY_SET_ID_PATH = "/{" + LINKING_ENTITY_SET_ID + "}";
    String LINKING_ID                 = "linkingId";
    String LINKING_ID_PATH            = "/{" + LINKING_ID + "}";
    String LINKS                      = "/links";
    String MATCHED                    = "/matched";
    String SET                        = "/set";

    String SERVICE = "/linker";
    String BASE    = SERVICE + CONTROLLER;

    @GET( BASE + FINISHED + SET )
    Set<UUID> getLinkingFinishedEntitySets();

    @GET( BASE + MATCHED + LINKING_ID_PATH )
    Set<MatchedEntityPair> getMatchedEntitiesForLinkingId( @Path( LINKING_ID ) UUID linkingId );

    @POST( BASE + BLOCKING )
    Map<UUID, Map<UUID, List<BlockedEntity>>> block( @Body BlockingRequest blockingRequest );

    UUID createNewLinkedEntity( Set<EntityDataKey> entityDataKeys );

    @GET( BASE + LINKS + LINKING_ENTITY_SET_ID_PATH)
    Map<UUID, Set<EntityDataKey>> getLinkedEntityKeyIds(
            @Path( LINKING_ENTITY_SET_ID ) UUID linkingEntitySetId );

    @PUT( BASE + LINKS + LINKING_ENTITY_SET_ID_PATH + LINKING_ID_PATH )
    Integer setLinkedEntities(
            @Path( LINKING_ENTITY_SET_ID ) UUID linkingEntitySetId,
            @Path( LINKING_ID ) UUID linkedEntityKeyId,
            @Body Set<EntityDataKey> entityDataKeys );

    @POST( BASE + LINKS + LINKING_ENTITY_SET_ID_PATH + LINKING_ID_PATH )
    Set<EntityDataKey> addLinkedEntities(
            @Path( LINKING_ENTITY_SET_ID ) UUID linkingEntitySetId,
            @Path( LINKING_ID ) UUID linkedEntityKeyId,
            @Body Set<EntityDataKey> entityDataKeys );

    @HTTP( method = "DELETE",
            path = BASE + LINKS + LINKING_ENTITY_SET_ID_PATH + LINKING_ID_PATH )
    Set<EntityDataKey> removeLinkedEntities(
            @Path( LINKING_ENTITY_SET_ID ) UUID linkingEntitySetId,
            @Path( LINKING_ID ) UUID linkedEntityKeyId,
            @Body Set<EntityDataKey> entityDataKeys );

}
