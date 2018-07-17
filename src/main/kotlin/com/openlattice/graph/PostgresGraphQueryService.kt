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
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.query.GraphQuery
import com.openlattice.graph.query.GraphQueryState
import com.openlattice.graph.query.ResultSummary
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.GRAPH_QUERIES
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresGraphQueryService(
        private val hds: HikariDataSource,
        private val edm: EdmManager,
        private val authorizationManager: AuthorizationManager,
        private val mapper: ObjectMapper
) : GraphQueryService {
    override fun getQuery(queryId: UUID): GraphQuery {
        val conn = hds.connection
        conn.use {
            val ps = conn.prepareStatement(getQuerySql)
            ps.setObject(1, queryId)
            val rs = ps.executeQuery()
            val json = rs.getString(QUERY.name)
            return mapper.readValue(json, GraphQuery::class.java)
        }
    }

    override fun getQueryState(queryId: UUID, options: Set<GraphQueryState.Option>): GraphQueryState {
        val queryState = getQueryState(queryId)
        if (options.contains(GraphQueryState.Option.RESULT)) {
            queryState.result = getResult(queryId)
        }

        if (options.contains(GraphQueryState.Option.SUMMARY)) {
            queryState.resultSummary = getResultSummary(queryId)
        }
        return queryState
    }

    override fun abortQuery(queryId: UUID) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


    override fun submitQuery(query: GraphQuery): GraphQueryState {
        val queryId = UUID.randomUUID()
        val startTime = saveQuery(queryId, query)
        //TODO: Consider stronger of enforcement of uniqueness for mission critical
        val visitor = EntityQueryExecutingVisitor(hds, edm,authorizationManager, queryId)
        query.entityQueries.forEach(visitor)
        val queryMap = visitor.queryMap
        discard(visitor.queryId, query.entityQueries.map { visitor.queryMap[it]!! })
        val queryState = GraphQueryState(
                visitor.queryId,
                GraphQueryState.State.RUNNING,
                System.currentTimeMillis() - startTime
        )
        return queryState
    }

    private fun saveQuery(queryId: UUID, query: GraphQuery): Long {
        val startTime = System.currentTimeMillis()
        val queryJson = mapper.writeValueAsString(query);
        val conn = hds.connection

        conn.use {
            val ps = conn.prepareStatement(insertGraphQuery)
            ps.setObject(1, queryId)
            ps.setString(2, queryJson)
            ps.setString(3, GraphQueryState.State.RUNNING.name)
            ps.setLong(4, startTime)
            //TODO: Consider checking to make sure value was inserted.
            ps.executeUpdate()
        }

        return startTime
    }


    private fun getQueryState(queryId: UUID): GraphQueryState {
        val conn = hds.connection
        conn.use {
            val ps = conn.prepareStatement(readGraphQueryState)
            ps.setObject(1, queryId)
            val rs = ps.executeQuery()
            return ResultSetAdapters.graphQueryState(rs)
        }
    }


    private fun getResultSummary(queryId: UUID): Optional<ResultSummary> {
        //Retrieve the query so we can compute the result summary
        val query = getQuery(queryId)

        //

        return Optional.empty()

    }

    override fun getResult(queryId: UUID): Optional<SubGraph> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /**
     * This function discards entity keys that didn't contribute to a top level clause.
     * @param queryId The query id to which this applies
     * @param clauseIds A list of top level clause ids.
     */
    private fun discard(queryId: UUID, clauseIds: List<Int>) {

    }
}

const val TTL_MILLIS = 10 * 60 * 1000
private val getQuerySql = "SELECT ${QUERY.name} FROM ${GRAPH_QUERIES.name} WHERE ${QUERY_ID.name} = ?"
private val readGraphQueryState = "SELECT * FROM ${GRAPH_QUERIES.name} WHERE ${QUERY_ID.name} = ?"
private val insertGraphQuery =
        "INSERT INTO ${GRAPH_QUERIES.name} (${QUERY_ID.name},${QUERY.name},${STATE.name},${START_TIME.name}) " +
                "VALUES (?,?,?,?)"