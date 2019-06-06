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

package com.openlattice.graph

import com.google.common.collect.ListMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.graph.query.GraphQuery
import com.openlattice.graph.query.GraphQueryState
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.*


//@formatter:off
const val SERVICE = "/datastore"
const val CONTROLLER = "/graph"
const val BASE = SERVICE + CONTROLLER
//@formatter:on

const val NEIGHBORS = "neighbors"
const val ENTITY_SET_ID = "entitySetId"
const val ENTITY_SET_ID_PATH = "/{$ENTITY_SET_ID}"
const val ID = "id"
const val ID_PATH = "/{$ID}"
const val QUERY = "/query"
const val RESULT = "/result"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface GraphApi {

    /**
     * Executes a search for all neighbors of multiple entities of the same entity set that are connected by an
     * association
     *
     * @param entitySetId The entity set id of the entities
     * @param query      Constraints and filters on the neighborhood to return
     * @return A map from each entity id to a list of objects containing information about the neighbors and
     * associations of that entity
     */
    @POST(BASE + NEIGHBORS + ENTITY_SET_ID_PATH)
    fun neighborhoodQuery(
            @Path(ENTITY_SET_ID) entitySetId: UUID,
            @Body query: NeighborhoodQuery
    ): Map<UUID, List<NeighborEntityDetails>>
}