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

package com.openlattice.linking;

import com.openlattice.data.DataApi;
import com.openlattice.data.EntityKey;
import com.openlattice.edm.type.LinkingEntityType;
import com.openlattice.linking.requests.LinkingRequest;
import java.util.Set;
import java.util.UUID;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;

/**
 * This API is used for creating and managing synthetic entity sets created by linking several entity sets together. The
 * entity sets created by this invoking this API are not actually instantiated until they are accessed via the
 * {@link DataApi}, which dynamically weaves them together based on the matching information.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface LinkingApi {
    String CONTROLLER = "/linking";
    String ENTITY_ID        = "entityId";
    String LINKED_ENTITY_ID = "linkedEntityId";
    /*
     * These determine the service routing for the LB
     */
    String SERVICE    = "/datastore";
    String BASE       = SERVICE + CONTROLLER;
    String SET              = "set";
    String SET_ID           = "setId";
    String SYNC_ID          = "syncId";
    String TYPE             = "type";

    /**
     * Creates an entity type describing the result of linking several entity types together. This entity type will be
     * be usable in entity set searches.
     *
     * @param linkingEntityType The linking entity type
     * @return The {@link UUID} of the linked entity type.
     */
    @POST( BASE + "/" + TYPE )
    UUID createLinkingEntityType( LinkingEntityType linkingEntityType );

    /**
     * Performs linking operation on entity sets.
     *
     * @param linkingRequest A request including a set of property type ids to populate in the linking result, and a
     * linking entity set definition consisting of an entity set and associated properties to link on. Each
     * map in linking properties must be the same length, with entity set ids as a keys and property type ids
     * as values. If no maps are provided, an empty linking entity set is created that can be populated by
     * calling {@link LinkingApi#linkEntities(UUID, UUID, Set)}.
     * @return The id of the new entity set constructed from linking the desired entity sets.
     */
    @POST( BASE )
    UUID linkEntitySets( @Body LinkingRequest linkingRequest );

    /**
     * Links a set of entities into a new linked entity.
     *
     * @return The entity id of the new linked entity id
     */
    @POST( BASE + "/" + SET + "/{" + SET_ID + "}/{" + ENTITY_ID + "}" )
    UUID linkEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Body Set<EntityKey> entities );

    @PUT( BASE + "/" + SET + "/{" + SET_ID + "}/{" + ENTITY_ID + "}" )
    Void setLinkedEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Body Set<EntityKey> entities );

    @DELETE( BASE + "/" + SET + "/{" + SET_ID + "}/{" + ENTITY_ID + "}" )
    Void deleteLinkedEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId );

    @PUT( BASE + "/" + SET + "/{" + SET_ID + "}/{" + ENTITY_ID + "}/{" + LINKED_ENTITY_ID + "}" )
    Void addLinkedEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Path( LINKED_ENTITY_ID ) UUID linkedEntityId );

    @DELETE( BASE + "/" + SET + "/{" + SET_ID + "}/{" + ENTITY_ID + "}/{" + LINKED_ENTITY_ID + "}" )
    Void removeLinkedEntity(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Path( LINKED_ENTITY_ID ) UUID linkedEntityId );

}
