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
 *
 */

package com.openlattice.entitysets

import retrofit2.http.*
import java.util.*


/*
* These determine the service routing for the LB
*/
const val SERVICE = "/datastore"
const val CONTROLLER = "/entity_sets"
const val BASE = SERVICE + CONTROLLER

const val ID = "id"
const val LEVEL = "level"
const val ID_PATH = "/$ID"

const val LINKED_PATH = "/linked"
const val ID_PATH_PARAM = "/{$ID}"

enum class Level {
    ID,
    FULL
}

/**
 * This API is for managing entity set metadata.
 */
interface EntitySetsApi {

    /**
     * Retrieves the underlying linked entity sets for a given set.
     * @param entitySetId The id of the linking entity set .
     * @param level Determines the amount of information returned by the call.
     *
     * @exception 404 If the entity set specified by entitySetId is not found.
     * @exception 400 If the entity set is not a linking entity set.
     * @return Either a Set<UUID> or a Set<EntitySet> depending on the level provided in the API call.
     */
    @GET(BASE + ID_PATH_PARAM + LINKED_PATH)
    fun getLinkingEntitySets(@Path(ID) entitySetId: UUID, @Query(LEVEL) level: Level): Set<Any>

    /**
     * Adds one ore more linked enitty to a given set.
     *
     * @param entitySetId The id of the linking entity set .
     * @param level Determines the amount of information returned by the call.
     * @return Either a Set<UUID> or a Set<EntitySet> depending on the level provided in the API call.
     *
     * @exception 404 If the entity set specified by entitySetId is not found.
     * @exception 400 If the entity set is not a linking entity set.
     */
    @PUT(BASE + ID_PATH + LINKED_PATH)
    fun addLinkingEntitySets(@Path(ID) entitySetId: UUID, @Body linkedEntitySets: Set<UUID>): Int

    /**
     * Removes one or more linked entity sets from a given set.
     *
     * @param entitySetId  The id of the linking entity set .
     * @param level Determines the amount of information returned by the call.
     * @return 200 OK and the number of entity sets actually removed.
     *
     * @exception 404 If the entity set specified by entitySetId is not found.
     * @exception 400 If the entity set is not a linking entity set.
     */
    @DELETE(BASE + ID_PATH + LINKED_PATH)
    fun removeLinkingEntitySets(@Path(ID) entitySetId: UUID, @Body linkedEntitySets: Set<UUID>): Int

}