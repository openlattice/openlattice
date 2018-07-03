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
import com.openlattice.graph.GraphApi
import com.openlattice.graph.SubGraph
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.query.GraphQuery
import com.openlattice.graph.query.Result
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.RequestMapping
import java.util.*
import javax.inject.Inject

/**
 *
 */
@Controller
@RequestMapping(GraphApi.CONTROLLER)
open class GraphController
@Inject
constructor(private val graphService: GraphService) : GraphApi {
    @Timed
    override fun query(query: GraphQuery?): Result {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Timed
    override fun getResults(queryId: UUID?): SubGraph {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    @Timed
    override fun graphQuery(ops: GraphQuery?): ListMultimap<UUID, SetMultimap<UUID, SetMultimap<UUID, Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}
