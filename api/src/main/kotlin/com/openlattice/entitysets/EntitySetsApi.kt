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
 */

package com.openlattice.entitysets

import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.EntitySetPropertyMetadata
import com.openlattice.edm.type.PropertyType
import retrofit2.http.*
import java.util.*


/**
 * This API is for managing entity sets.
 */
interface EntitySetsApi {

    companion object {
        /* These determine the service routing */
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/entity-sets"
        const val BASE = SERVICE + CONTROLLER

        const val ALL = "/all"
        const val LINKING = "/linking"

        const val ID = "id"
        const val ID_PATH = "/{$ID}"


        const val IDS_PATH = "/ids"

        const val NAME = "name"
        const val NAME_PATH = "/{$NAME}"
        const val PROPERTIES_PATH = "/properties"
        const val METADATA_PATH = "/metadata"
        const val EXPIRATION_PATH = "/expiration"

        const val PROPERTY_TYPE_ID = "propertyTypeId"
        const val PROPERTY_TYPE_ID_PATH = "/{$PROPERTY_TYPE_ID}"
    }

    /**
     * Gets all entity sets available to the calling user.
     *
     * @return Iterable containing entity sets available to the calling user.
     */
    @GET(BASE)
    fun getEntitySets(): Set<EntitySet>

    /**
     * Creates multiple entity sets if they do not exist.
     *
     * @param entitySets The entity sets to create.
     * @return The entity sets created with UUIDs.
     */
    @POST(BASE)
    fun createEntitySets(@Body entitySets: Set<@JvmSuppressWildcards EntitySet>): Map<String, UUID>


    /**
     * Get IDs for entity sets given their names.
     *
     * @param entitySetNames The names of the entity sets.
     * @return Ids of entity sets.
     */
    @POST(BASE + IDS_PATH)
    fun getEntitySetIds(@Body entitySetNames: Set<String>): Map<String, UUID>

    /**
     * Get ID for entity set with given name.
     *
     * @param entitySetName The name of the entity set.
     * @return ID for entity set.
     */
    @GET(BASE + IDS_PATH + NAME_PATH)
    fun getEntitySetId(@Path(NAME) entitySetName: String): UUID

    /**
     * Hard deletes the entity set
     *
     * @param entitySetId the ID for the entity set
     * @return The number of entities deleted.
     */
    @DELETE(BASE + ALL + ID_PATH)
    fun deleteEntitySet(@Path(ID) entitySetId: UUID): Int

    /**
     * Get entity set id, entity type id, name, title, description, and contacts list for a given entity set.
     *
     * @param entitySetId The id for the entity set.
     * @return The details of the entity set with the specified id.
     */
    @GET(BASE + ALL + ID_PATH)
    fun getEntitySet(@Path(ID) entitySetId: UUID): EntitySet

    @POST(BASE + ALL + METADATA_PATH)
    fun getPropertyMetadataForEntitySets(@Body entitySetIds: Set<UUID>): Map<UUID, Map<UUID, EntitySetPropertyMetadata>>

    @GET(BASE + ALL + ID_PATH + METADATA_PATH)
    fun getAllEntitySetPropertyMetadata(@Path(ID) entitySetId: UUID): Map<UUID, EntitySetPropertyMetadata>

    @GET(BASE + ALL + ID_PATH + PROPERTIES_PATH)
    fun getPropertyTypesForEntitySet(@Path(ID) entitySetId: UUID): Map<UUID, PropertyType>

    @GET(BASE + ALL + ID_PATH + PROPERTIES_PATH + PROPERTY_TYPE_ID_PATH)
    fun getEntitySetPropertyMetadata(
            @Path(ID) entitySetId: UUID,
            @Path(PROPERTY_TYPE_ID) propertyTypeId: UUID
    ): EntitySetPropertyMetadata

    /**
     * Update the metadata for a property type in a given entity set.
     *
     * @param entitySetId The id of the entity set.
     * @param propertyTypeId The id of the property type.
     * @param update A [MetadataUpdate] object that describe the new values for the property type metadata.
     *
     * @return Number of updates made.
     */
    @POST(BASE + ALL + ID_PATH + PROPERTIES_PATH + PROPERTY_TYPE_ID_PATH)
    fun updateEntitySetPropertyMetadata(
            @Path(ID) entitySetId: UUID,
            @Path(PROPERTY_TYPE_ID) propertyTypeId: UUID,
            @Body update: MetadataUpdate
    ): Int

    /**
     * Updates the metadata for a given entity set.
     *
     * @param entitySetId ID for entity set.
     * @param update      Only title, description, contacts and name fields are accepted. Other fields are ignored. This is
     * somewhat out of date.
     */
    @PATCH(BASE + ALL + ID_PATH + METADATA_PATH)
    fun updateEntitySetMetadata(@Path(ID) entitySetId: UUID, @Body update: MetadataUpdate): Int

    /**
     * Adds the entity sets as linked entity sets to the linking entity set
     * @param linkingEntitySetId the id of the linking entity set
     * @param entitySetIds the ids of the entity sets to be linked
     */
    @PUT(BASE + LINKING + ID_PATH)
    fun addEntitySetsToLinkingEntitySet(@Path(ID) linkingEntitySetId: UUID, @Body entitySetIds: Set<UUID>): Int

    /**
     * Adds the entity sets as linked entity sets to the linking entity sets
     * @param entitysetIds mapping of the ids of the entity sets to be linked with keys of the linking entity sets
     */
    @POST(BASE + LINKING)
    fun addEntitySetsToLinkingEntitySets(@Body entitySetIds: Map<UUID, Set<UUID>>): Int

    /**
     * Removes/unlinks the linked entity sets from the linking entity set
     * @param linkingEntitySetId the id of the linking entity set
     * @param entitysetIds the ids of the entity sets to be removed/unlinked
     */
    @HTTP(method = "DELETE", path = BASE + LINKING + ID_PATH, hasBody = true)
    fun removeEntitySetsFromLinkingEntitySet(@Path(ID) linkingEntitySetId: UUID, @Body entitySetIds: Set<UUID>): Int

    /**
     * Removes/unlinks the linked entity sets as from the linking entity sets
     * @param entitysetIds mapping of the ids of the entity sets to be unlinked with keys of the linking entity sets
     */
    @HTTP(method = "DELETE", path = BASE + LINKING, hasBody = true)
    fun removeEntitySetsFromLinkingEntitySets(@Body entitySetIds: Map<UUID, Set<UUID>>): Int

    /**
     * Removes a data expiration policy previously set on an entity set.
     * @param entitySetId The id of the entity set.
     */
    @PATCH(BASE + ALL + ID_PATH + EXPIRATION_PATH)
    fun removeDataExpirationPolicy(@Path(ID) entitySetId: UUID): Int

    /**
     * Gets data from an entity set that will expire before a specified date
     * @param entitySetId The id of the entity set to check
     * @param dateTime The date time to check against (i.e. what entities will expire before this date time)
     * The dateTime is expected in the format "yyyy-MM-dd'T'HH:mm:ss.SSS-ZZ:ZZ"
     *
     * @return Set of entity key ids that will expire before the specified date time
     */
    @POST(BASE + ALL + ID_PATH + EXPIRATION_PATH )
    fun getExpiringEntitiesFromEntitySet(@Path(ID) entitySetId: UUID,
                                         @Body dateTime: String): Set<UUID>

}