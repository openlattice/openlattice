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

import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.query.AbstractEntityQuery
import com.openlattice.graph.query.EntityKeyIdQuery
import com.openlattice.graph.query.EntityQuery
import com.openlattice.graph.query.EntitySetQuery
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ENTITY_QUERIES
import com.zaxxer.hikari.HikariDataSource
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * Vistor class that executes initial populating queries for a graph query.
 */
class EntityQueryExecutingVisitor(
        private val hds: HikariDataSource,
        private val edm: EdmManager,
        val queryId: UUID
) : EntityQueryVisitor {
    private val pgeqs = PostgresEntityDataQueryService(hds)
    private val indexGen = AtomicInteger()
    private val expiry = System.currentTimeMillis() + 10 * 60 * 1000 //10 minute, arbitary
    val queryMap: MutableMap<EntityQuery, Int> = mutableMapOf()

    override fun accept(query: EntityQuery) {
        query.childQueries.forEach(this)
        when (query) {
            is EntitySetQuery -> executeQuery(query)
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
        //An entity key id query is easy to satisfy by plugging in entity key ids.
        //val currentClauseIndex = assignIndex(query)
        val sql = "INSERT INTO ${ENTITY_QUERIES.name} " +
                "(${QUERY_ID.name},${ID_VALUE.name},${CLAUSES.name},${START_TIME.name}) " +
                "VALUES ($queryId,${query.entityKeyId},ARRAY[${query.id}],$expiry) " +
                "ON CONFLICT((${QUERY_ID.name},${ID_VALUE.name}) " +
                "DO UPDATE SET ${ENTITY_QUERIES.name}.${CLAUSES.name} = " +
                "${ENTITY_QUERIES.name}.${CLAUSES.name} || EXCLUDED.${CLAUSES.name}  "

        val connection = hds.connection
        connection.use {
            connection.createStatement().executeUpdate(sql)
        }
    }

    /**
     * Executes an entity set query.
     * @param query The entity set query to execute.
     */
    private fun executeQuery(query: EntitySetQuery) {
        val clausesVisitor = ClauseBuildingVisitor()
        val clauses = clausesVisitor.apply(query.clauses)
        val entitySetIds = query.entitySetId
                .map { setOf(it) }
                .orElse(edm.getEntitySetsOfType(query.entityTypeId).map { it.id }.toSet())
        //private val authorizedPropertyTypes: Map<UUID, PropertyType>,
        //For each entity set we have to execute a boolean query and insert it into
////        val selectSqls = entitySetIds.map( pgeqs::)
////
////        val sql = "INSERT INTO ${ENTITY_QUERIES.name} " +
////                "(${QUERY_ID.name},${ID_VALUE.name},${CLAUSES.name},${START_TIME.name}) " +
////                "VALUES ($queryId,${query.entityKeyId},ARRAY[${query.id}],$expiry) " +
////                "ON CONFLICT((${QUERY_ID.name},${ID_VALUE.name}) " +
////                "DO UPDATE SET ${ENTITY_QUERIES.name}.${CLAUSES.name} = " +
////                "${ENTITY_QUERIES.name}.${CLAUSES.name} || EXCLUDED.${CLAUSES.name}  "
//
//
//        val connection = hds.connection
//        connection.use {
//            connection.createStatement().executeUpdate(sql)
//        }
    }


    private fun assignIndex(query: EntityQuery): Int {
        val currentId = indexGen.getAndIncrement()
        queryMap[query] = currentId
        return currentId
    }


}