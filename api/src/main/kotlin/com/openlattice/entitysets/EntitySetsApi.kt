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

import retrofit2.http.*

import java.util.UUID


/**
 * This API is for managing entity sets.
 */
interface EntitySetsApi {

    companion object {
        /* These determine the service routing */
        const val SERVICE = "/datastore"
        const val CONTROLLER = "/entity_sets"
        const val BASE = SERVICE + CONTROLLER

        const val SET_ID = "setId"
        const val SET_ID_PATH = "/{ $SET_ID }"

        const val LINKING = "/linking"
    }

    /**
     * Adds the entity sets as linked entity sets to the linking entity set
     * @param linkingEntitySetId the id of the linking entity set
     * @param entitysetIds the ids of the entity sets to be linked
     */
    @PUT(BASE + LINKING + SET_ID_PATH)
    fun addEntitySetsToLinkingEntitySet(@Path(SET_ID) linkingEntitySetId: UUID, @Body entitySetIds: Set<UUID>): Int

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
    @HTTP(method = "DELETE", path = BASE + LINKING + SET_ID_PATH, hasBody = true)
    fun removeEntitySetsFromLinkingEntitySet(@Path(SET_ID) linkingEntitySetId: UUID, @Body entitySetIds: Set<UUID>): Int

    /**
     * Removes/unlinks the linked entity sets as from the linking entity sets
     * @param entitysetIds mapping of the ids of the entity sets to be unlinked with keys of the linking entity sets
     */
    @HTTP(method = "DELETE", path = BASE + LINKING, hasBody = true)
    fun removeEntitySetsFromLinkingEntitySets(@Body entitySetIds: Map<UUID, Set<UUID>>): Int

}