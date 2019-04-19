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
 *
 */

package com.openlattice.admin.indexing

import com.openlattice.admin.RELOAD_CACHE
import com.openlattice.authorization.Principal
import retrofit2.http.*
import java.util.*


// @formatter:off
const val SERVICE = "/datastore"
const val CONTROLLER = "/index"
const val BASE = SERVICE + CONTROLLER
// @formatter:on

const val REINDEX = "/reindex"
const val PRINCIPALS = "/principals"

const val ID = "id"
const val ID_PATH = "/{$ID}"
const val NAME = "name"
const val NAME_PATH = "/{$NAME}"

interface IndexAdminApi {

    /**
     * Retrieve the current state of reindexing jobs
     */
    @GET(BASE + REINDEX)
    fun getIndexingState() : IndexingState

    /**
     * Merge job descriptions for performing a partial or full reindex of provided entity sets.
     *
     * @param reindexRequest A map of entity set ids to entity key ids that determines what will be reindexed. If no
     * entity key ids are provided then all entities in an entity set are reindexed. If no entity set ids are provided
     * then all entity sets are scheduled for reindexing.
     */
    @POST(BASE + REINDEX )
    fun reindex(@Body reindexRequest: Map<UUID, Set<UUID>> ) : IndexingState

    /**
     * Replaces job descriptions for performing a partial or full reindex of provided entity sets.
     *
     * @param reindexRequest A map of entity set ids to entity key ids that determines what will be reindexed. If no
     * entity key ids are provided then all entities in an entity set are reindexed. If no entity set ids are provided
     * then all entity sets are scheduled for reindexing.
     */
    @PUT(BASE + REINDEX )
    fun updateReindex(@Body reindexRequest: Map<UUID, Set<UUID>> ) : IndexingState

    /**
     * Clears all the data currently being indexed in the queue.
     * @return The number of items in the queue before being cleared.
     */
    @DELETE( BASE + REINDEX )
    fun clearIndexingQueue() : Int

}