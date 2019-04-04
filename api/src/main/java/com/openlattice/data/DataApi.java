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

import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import retrofit2.http.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface DataApi {
    // @formatter:off
    String SERVICE               = "/datastore";
    String CONTROLLER            = "/data";
    String BASE                  = SERVICE + CONTROLLER;
    // @formatter:on

    String ASSOCIATION           = "association";

    int    MAX_BATCH_SIZE        = 500;
    String COUNT                 = "count";
    String ENTITY_KEY_ID         = "entityKeyId";
    String ENTITY_KEY_ID_PATH    = "{" + ENTITY_KEY_ID + "}";
    /**
     * To discuss paths later; perhaps batch this with EdmApi paths
     */

    String ALL                   = "all";
    String PROPERTIES            = "properties";
    String ENTITY_SET            = "set";
    String ENTITY_SET_ID         = "setId";
    String S3_URL                = "s3Url";
    String S3_URLS               = "s3Urls";
    String FILE_TYPE             = "fileType";
    String NEIGHBORS             = "neighbors";
    String PARTIAL               = "partial";
    String PROPERTY_TYPE_ID      = "propertyTypeId";
    String PROPERTY_TYPE_ID_PATH = "{" + PROPERTY_TYPE_ID + "}";
    /*
     * These determine the service routing for the LB
     */
    String SET_ID_PATH           = "{" + ENTITY_SET_ID + "}";
    String S3_URL_PATH           = "{" + S3_URL + "}";
    String S3_URLS_PATH          = "{" + S3_URLS + "}";
    String TOKEN                 = "token";
    String TYPE                  = "type";
    String UPDATE                = "update";

    @GET( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH )
    Iterable<SetMultimap<FullQualifiedName, Object>> loadEntitySetData(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Query( FILE_TYPE ) FileType fileType,
            @Query( TOKEN ) String token );

    /**
     * @param req If syncId is not specified in the request, will retrieve the data from the current syncIds. If
     *            selectedProperties are not specified, all readable properties will be fetched.
     * @return An iterable containing the entity data, using property type FQNs as keys
     */
    @POST( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH )
    Iterable<SetMultimap<FullQualifiedName, Object>> loadEntitySetData(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Body EntitySetSelection req,
            @Query( FILE_TYPE ) FileType fileType );

    @POST( BASE + "/" + ENTITY_SET + "/" )
    List<UUID> createEntities(
            @Query( ENTITY_SET_ID ) UUID entitySetId,
            @Body List<Map<UUID, Set<Object>>> entities );

    /**
     * Replaces a single entity from an entity set.
     *
     * @param entitySetId The id of the entity set the entity belongs to.
     * @param entityKeyId The id of the entity to replace.
     * @param entity      The new entity details object that will be merged into old values, with property type ids as keys.
     */
    @PUT( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    Integer mergeIntoEntityInEntitySet(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Body Map<UUID, Set<Object>> entity );

    /**
     * Replaces a single entity from an entity set.
     *
     * @param entitySetId The id of the entity set the entity belongs to.
     * @param entityKeyId The id of the entity to replace.
     * @param entity      The new entity details object that will replace the old value, with property type ids as keys.
     */
    @PUT( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    Integer replaceEntityInEntitySet(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Body Map<UUID, Set<Object>> entity );

    /**
     * Perform one of the following bulk update operations on entities.
     *
     * <ul>
     * <li>{@link UpdateType#Merge} adds new properties without affecting existing data.</li>
     * <li>{@link UpdateType#PartialReplace} replaces all values for supplied property types, but does not not affect
     * other property types for an entity</li>
     * <li>{@link UpdateType#Replace} replaces all entity data with the supplied properties.</li>
     * </ul>
     *
     * @param entitySetId The id of the entity set to write to.
     * @param entities    A map of entity key ids to entities to merge
     * @param updateType  The update type to perform.
     * @return The total number of entities updated.
     */
    @PUT( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH )
    Integer updateEntitiesInEntitySet(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Body Map<UUID, Map<UUID, Set<Object>>> entities,
            @Query( TYPE ) UpdateType updateType );

    @PATCH( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH )
    Integer replaceEntityProperties(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Body Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Object>>> entities );

    /**
     * Creates a new set of associations.
     *
     * @param associations Set of associations to create. An association is the triple of source, destination, and edge
     *                     entitiy key ids.
     */
    @PUT( BASE + "/" + ASSOCIATION )
    Integer createAssociations( @Body Set<DataEdgeKey> associations );

    /**
     * Creates a new set of associations.
     *
     * @param associations Set of associations to create. Keys are association entity set ids and values for each keys
     *                     are the data to be created.
     */
    @POST( BASE + "/" + ASSOCIATION )
    ListMultimap<UUID, UUID> createAssociations( @Body ListMultimap<UUID, DataEdge> associations );

    @PATCH( BASE + "/" + ASSOCIATION )
    Integer replaceAssociationData(
            @Body Map<UUID, Map<UUID, DataEdge>> associations,
            @Query( PARTIAL ) boolean partial );

    @POST( BASE )
    DataGraphIds createEntityAndAssociationData( @Body DataGraph data );


    /**
     * Clears the Entity matching the given Entity id and all of its neighbor Entities
     *
     * @param vertexEntitySetId the id of the EntitySet to delete from
     * @param vertexEntityKeyId the id of the Entity to delete
     */
    @DELETE( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + NEIGHBORS )
    Long clearEntityAndNeighborEntities(
            @Path( ENTITY_SET_ID ) UUID vertexEntitySetId,
            @Path( ENTITY_KEY_ID ) UUID vertexEntityKeyId
    );

    /**
     * Deletes all entities from an entity set.
     *
     * @param entitySetId The id of the entity set to delete from.
     * @param deleteType  The delete type to perform (soft or hard delete).
     */
    @DELETE( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ALL )
    Integer deleteAllEntitiesFromEntitySet(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Query( TYPE ) DeleteType deleteType );

    /**
     * Deletes a single entity from an entity set.
     *
     * @param entitySetId The id of the entity set to delete from.
     * @param entityKeyId The id of the entity to delete.
     * @param deleteType  The delete type to perform (soft or hard delete).
     */
    @DELETE( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    Integer deleteEntity(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Query( TYPE ) DeleteType deleteType );

    /**
     * Deletes multiple entities from an entity set.
     *
     * @param entitySetId  The id of the entity set to delete from.
     * @param entityKeyIds The ids of the entities to delete.
     * @param deleteType   The delete type to perform (soft or hard delete).
     */
    @HTTP( method = "DELETE", path = BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH, hasBody = true )
    Integer deleteEntities(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Body Set<UUID> entityKeyIds,
            @Query( TYPE ) DeleteType deleteType );

    /**
     * Deletes properties from an entity.
     *
     * @param entitySetId     The id of the entitySet to delete from.
     * @param entityKeyId     The id of the entity to delete from.
     * @param propertyTypeIds The property type ids to be deleted.
     * @param deleteType      The delete type to perform (soft or hard delete).
     * @return the number of deleted property values
     */
    @HTTP(
            method = "DELETE",
            path = BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTIES,
            hasBody = true
    )
    Integer deleteEntityProperties(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Body Set<UUID> propertyTypeIds,
            @Query( TYPE ) DeleteType deleteType );

    /**
     * Replaces a single entity from an entity set.
     *
     * @param entitySetId  The id of the entity set the entity belongs to.
     * @param entityKeyId  The id of the entity to replace.
     * @param entityByFqns The new entity details object that will replace the old value, with property type FQNs as keys.
     */
    @POST( BASE + "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    Integer replaceEntityInEntitySetUsingFqns(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Body Map<FullQualifiedName, Set<Object>> entityByFqns );

    /**
     * Gets the number of entities in an entity set.
     *
     * @param entitySetId The id of the entity set to return a count for.
     * @return The number of entities in the entity set.
     */
    @GET( BASE + "/" + SET_ID_PATH + "/" + COUNT )
    long getEntitySetSize( @Path( ENTITY_SET_ID ) UUID entitySetId );

    /**
     * Loads a single entity by its entityKeyId and entitySetId
     *
     * @param entitySetId The entity set which the request entity belongs to.
     * @param entityKeyId The id of the requested entity.
     * @return A enttity details object, with property type FQNs as keys.
     */
    @GET( BASE + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH )
    SetMultimap<FullQualifiedName, Object> getEntity(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId );

    @GET( BASE + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTY_TYPE_ID_PATH )
    Set<Object> getEntity(
            @Path( ENTITY_SET_ID ) UUID entitySetId,
            @Path( ENTITY_KEY_ID ) UUID entityKeyId,
            @Path( PROPERTY_TYPE_ID ) UUID propertyTypeId );
}
