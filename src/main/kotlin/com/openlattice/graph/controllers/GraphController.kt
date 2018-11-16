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

package com.openlattice.graph.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.ListMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.graph.GraphApi
import com.openlattice.graph.GraphApi.ID
import com.openlattice.graph.GraphApi.ID_PATH
import com.openlattice.graph.GraphApi.QUERY
import com.openlattice.graph.GraphQueryService
import com.openlattice.graph.SimpleGraphQuery
import com.openlattice.graph.SubGraph
import com.openlattice.graph.query.GraphQuery
import com.openlattice.graph.query.GraphQueryState
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

/**
 *
 */
@RestController
@RequestMapping(GraphApi.CONTROLLER)
class GraphController
@Inject
constructor(
        private val graphQueryService: GraphQueryService,
        private val authorizationManager: AuthorizationManager
//        private val filtered
) : GraphApi, AuthorizingComponent {



    @Timed
    @PostMapping(
            value = QUERY,
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submit(query: SimpleGraphQuery): GraphQueryState {
        //Collect the data to authorize

        //Collect the things to perserve
//        return graphQueryService.submitQuery(query);
        TODO("Not implemented")
    }

    @Timed
    @PostMapping(
            value = QUERY + ID_PATH,
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getQueryState(
            @PathVariable(ID) queryId: UUID,
            @RequestBody options: Set<GraphQueryState.Option>
    ): GraphQueryState {
        return graphQueryService.getQueryState(queryId, options)
    }

    override fun getQueryState(queryId: UUID): GraphQueryState {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Timed
    override fun getResults(queryId: UUID): SubGraph {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Timed
    override fun graphQuery(ops: GraphQuery): ListMultimap<UUID, SetMultimap<UUID, SetMultimap<UUID, Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}
