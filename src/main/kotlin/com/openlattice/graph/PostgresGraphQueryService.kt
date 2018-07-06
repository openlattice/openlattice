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

import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.graph.query.GraphQuery
import com.openlattice.graph.query.GraphQueryState
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.GRAPH_QUERIES
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresGraphQueryService(
        private val hds: HikariDataSource, private val mapper: ObjectMapper
) : GraphQueryService {
    override fun getQuery(queryId: UUID): GraphQuery {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getQueryState(queryId: UUID): UUID {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun abortQuery(queryId: UUID) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun submitQuery(query: GraphQuery): UUID {
        val queryId = UUID.randomUUID()
        saveQuery(queryId, query);
        //TODO: Consider stronger of enforcement of uniqueness for mission critical
        val visitor = EntityQueryExecutingVisitor(hds, queryId)
        query.entityQueries.forEach { it.dfs(visitor) }
        val queryMap = visitor.queryMap
        discard(visitor.queryId, query.entityQueries.map { visitor.queryMap[it]!! })
        return visitor.queryId
    }


    private fun saveQuery(queryId: UUID, query: GraphQuery) {
        val queryJson = mapper.writeValueAsString(query);
        val conn = hds.connection
        conn.use {
            val ps = conn.prepareStatement(insertGraphQuery)
            ps.setObject(1, queryId)
            ps.setString(2, queryJson)
            ps.setString(3, GraphQueryState.State.RUNNING.name)
            ps.setObject(4, System.currentTimeMillis() + TTL_MILLIS)
            //TODO: Consider checking to make sure value was inserted.
            ps.executeUpdate()
        }
    }

    /**
     * This function discards entity keys that didn't contribute to a top level clause.
     * @param queryId The query id to which this applies
     * @param clauseIds A list of top level clause ids.
     */
    private fun discard(queryId: UUID, clauseIds: List<Int>) {

    }
}

const val TTL_MILLIS = 10 * 60 * 1000;
private val insertGraphQuery =
        "INSERT INTO ${GRAPH_QUERIES.name} (${QUERY_ID.name},${QUERY.name},${STATE.name},${EXPIRY.name}) " +
                "VALUES (?,?,?,?)"