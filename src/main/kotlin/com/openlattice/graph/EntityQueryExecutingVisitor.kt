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

import com.openlattice.graph.query.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ENTITY_QUERIES
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EntityQueryExecutingVisitor(private val hds: HikariDataSource, val queryId: UUID) : EntityQueryVisitor {
    private val indexGen = AtomicInteger()
    private val expiry = System.currentTimeMillis() + 10 * 60 * 1000 //10 minute, arbitary
    val queryMap: MutableMap<EntityQuery, Int> = mutableMapOf()
    override fun accept(query: EntityQuery) {
        when (query) {
            is EntitySetQuery -> executeQuery(query)
            is EntityTypeQuery -> executeQuery(query)
            is EntityKeyIdQuery -> executeQuery(query)
            is AbstractEntityQuery.And -> executeQuery(query)
            is AbstractEntityQuery.Or -> executeQuery(query)
        }
    }

    private fun executeQuery(query: AbstractEntityQuery.Or) {
        val clauses = query.childQueries.map { queryMap[it]!! }
        val currentClauseIndex = assignIndex(query)
    }

    private fun executeQuery(query: AbstractEntityQuery.And) {
        val clauses = query.childQueries.map { queryMap[it]!! }
        val currentClauseIndex = assignIndex(query)

    }

    private fun executeQuery(query: EntityKeyIdQuery) {
        val currentClauseIndex = assignIndex(query)
        val sql = "INSERT INTO ${ENTITY_QUERIES.name} " +
                "(${QUERY_ID.name},${ID_VALUE.name},${CLAUSES.name},${EXPIRY.name}) " +
                "VALUES ($queryId,${query.entityKeyId},ARRAY[$currentClauseIndex],$expiry) " +
                "ON CONFLICT((${QUERY_ID.name},${ID_VALUE.name}) " +
                "DO UPDATE SET ${ENTITY_QUERIES.name}.${CLAUSES.name} = " +
                "s${ENTITY_QUERIES.name}.${CLAUSES.name} || EXCLUDED.${CLAUSES.name}  "

        val connection = hds.connection
        connection.use {
            connection.createStatement().executeUpdate(sql)
        }
    }

    private fun executeQuery(query: EntityTypeQuery) {

    }

    private fun executeQuery(query: EntitySetQuery) {

    }

    private fun assignIndex(query: EntityQuery): Int {
        val currentId = indexGen.getAndIncrement()
        queryMap[query] = currentId
        return currentId
    }


}