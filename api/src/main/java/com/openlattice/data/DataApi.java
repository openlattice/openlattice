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

package com.openlattice.data;

import com.openlattice.data.requests.Association;
import com.openlattice.data.requests.BulkDataCreation;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.google.common.collect.SetMultimap;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import retrofit2.http.*;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface DataApi {
    /*
     * These determine the service routing for the LB
     */
    String SERVICE    = "/datastore";
    String CONTROLLER = "/data";
    String BASE       = SERVICE + CONTROLLER;

    /**
     * To discuss paths later; perhaps batch this with EdmApi paths
     */

    String HISTORICAL       = "historical";
    String ENTITY_DATA      = "entitydata";
    String ASSOCIATION_DATA = "associationndata";

    String ENTITY_KEY_ID = "entityKeyId";
    String SET_ID        = "setId";
    String TICKET        = "ticket";
    String COUNT         = "count";
    String UPDATE        = "update";

    String ENTITY_KEY_ID_PATH = "{" + ENTITY_KEY_ID + "}";
    String SET_ID_PATH        = "{" + SET_ID + "}";
    String TICKET_PATH        = "{" + TICKET + "}";

    String FILE_TYPE = "fileType";
    String TOKEN     = "token";

    @POST( BASE + "/" + TICKET + "/" + SET_ID_PATH )
    UUID acquireSyncTicket( @Path( SET_ID ) UUID entitySetId  );

    @DELETE( BASE + "/" + TICKET + "/" + TICKET_PATH )
    Void releaseSyncTicket( @Path( TICKET ) UUID ticket );

    @PATCH( BASE + "/" + ENTITY_DATA + "/" + TICKET_PATH  )
    Void storeEntityData(
            @Path( TICKET ) UUID ticket,
            @Body Map<String, SetMultimap<UUID, Object>> entities );

    @GET( BASE + "/" + ENTITY_DATA + "/" + SET_ID_PATH )
    Iterable<SetMultimap<FullQualifiedName, Object>> loadEntitySetData(
            @Path( SET_ID ) UUID entitySetId,
            @Query( FILE_TYPE ) FileType fileType,
            @Query( TOKEN ) String token );

    /**
     * @param entitySetId
     * @param req         If syncId is not specified in the request, will retrieve the data from the current syncIds. If
     *                    selectedProperties are not specified, all readable properties will be fetched.
     * @param fileType
     * @return An iterable containing the entity data, using property type FQNs as keys
     */
    @POST( BASE + "/" + ENTITY_DATA + "/" + SET_ID_PATH )
    Iterable<SetMultimap<FullQualifiedName, Object>> loadEntitySetData(
            @Path( SET_ID ) UUID entitySetId,
            @Body EntitySetSelection req,
            @Query( FILE_TYPE ) FileType fileType );

    /**
     * Creates a new set of entities.
     *
     * @param entitySetId The id of the entity set to write to.
     * @param entities    A map describing the entities to create. Each key will be used as the entity id and must be unique
     *                    and stable across repeated integrations of data. If either constraint is violated then data may be
     *                    overwritten or duplicated.
     * @return
     */
    @PUT( BASE + "/" + ENTITY_DATA + "/" + SET_ID_PATH )
    Void createEntityData(
            @Path( SET_ID ) UUID entitySetId,
            @Body Map<String, SetMultimap<UUID, Object>> entities );

    /**
     * Creates a new set of associations.
     *
     * @param entitySetId  The id of the edge entity set to write to.
     * @param associations Set of associations to create. An association is the usual (String entityId, SetMultimap &lt;
     *                     UUID, Object &gt; details of entity) pairing enriched with source/destination EntityKeys
     * @return
     */
    @PUT( BASE + "/" + ASSOCIATION_DATA + "/" + SET_ID_PATH  )
    Void createAssociationData(
            @Path( SET_ID ) UUID entitySetId,
            @Body Set<Association> associations );

    @PATCH( BASE + "/" + ASSOCIATION_DATA + "/" + TICKET_PATH )
    Void storeAssociationData(
            @Path( TICKET ) UUID ticket,
            @Body Set<Association> associations );

    @PATCH( BASE + "/" + ENTITY_DATA )
    Void createEntityAndAssociationData( @Body BulkDataCreation data );

    /**
     * Deletes a single entity from an entity set.
     *
     * @param entitySetId The id of the entity set to delete from.
     * @param entityKeyId The id of the entity to delete.
     * @return
     */
    @DELETE( BASE + "/" + ENTITY_DATA + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    Void deleteEntityFromEntitySet( @Path( SET_ID ) UUID entitySetId, @Path( ENTITY_KEY_ID ) UUID entityKeyId );

    /**
     * Replaces a single entity from an entity set.
     *
     * @param entitySetId The id of the entity set the entity belongs to.
     * @param entityKeyId The id of the entity to replace.
     * @param entity      The new entity details object that will replace the old value, with property type ids as keys.
     * @return
     */
    @PUT( BASE + "/" + ENTITY_DATA + "/" + UPDATE + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    Void replaceEntityInEntitySet(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Body SetMultimap<UUID, Object> entity );

    /**
     * Replaces a single entity from an entity set.
     *
     * @param entitySetId  The id of the entity set the entity belongs to.
     * @param entityKeyId  The id of the entity to replace.
     * @param entityByFqns The new entity details object that will replace the old value, with property type FQNs as keys.
     * @return
     */
    @POST( BASE + "/" + ENTITY_DATA + "/" + UPDATE + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    Void replaceEntityInEntitySetUsingFqns(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Body SetMultimap<FullQualifiedName, Object> entityByFqns );

    /**
     * Gets the number of entities in an entity set.
     *
     * @param entitySetId The id of the entity set to return a count for.
     * @return The number of entities in the entity set.
     */
    @GET( BASE + "/" + SET_ID_PATH + "/" + COUNT )
    long getEntitySetSize( @Path( SET_ID ) UUID entitySetId );

    /**
     * Loads a single entity by its entityKeyId and entitySetId
     *
     * @param entitySetId The entity set which the request entity belongs to.
     * @param entityKeyId The id of the requested entity.
     * @return A enttity details object, with property type FQNs as keys.
     */
    @GET( BASE + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    SetMultimap<FullQualifiedName, Object> getEntity(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId );
}
