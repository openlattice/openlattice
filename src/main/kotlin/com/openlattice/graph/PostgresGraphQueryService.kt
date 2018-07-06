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

package com.openlattice.graph

import com.openlattice.graph.query.GraphQuery
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class GraphQueryService(private val hds: HikariDataSource) {
    fun submitQuery(query: GraphQuery): UUID {
        //TODO: Consider stronger of enforcement of uniqueness for mission critical
        val visitor = EntityQueryExecutingVisitor(hds, UUID.randomUUID());
        query.entityQueries.forEach { it.dfs(visitor) }
        val queryMap = visitor.queryMap
        discard(visitor.queryId, query.entityQueries.map { visitor.queryMap[it]!! })
        return visitor.queryId
    }

    /**
     * This function discards entity keys that didn't contribute to a top level clause.
     * @param queryId The query id to which this applies
     * @param clauseIds A list of top level clause ids.
     */
    private fun discard(queryId: UUID, clauseIds: List<Int>) {

    }
}