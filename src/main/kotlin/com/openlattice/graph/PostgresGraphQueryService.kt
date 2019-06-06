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
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.query.GraphQuery
import com.openlattice.graph.query.GraphQueryState
import com.openlattice.graph.query.ResultSummary
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.EDGES
import com.openlattice.postgres.PostgresTable.GRAPH_QUERIES
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.security.InvalidParameterException
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresGraphQueryService(
        private val hds: HikariDataSource,
        private val edm: EdmManager,
        private val authorizationManager: AuthorizationManager,
        private val byteBlobDataManager: ByteBlobDataManager,
        private val mapper: ObjectMapper
) : GraphQueryService {
    override fun submitQuery(query: NeighborhoodQuery, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>) {
        val selectionIndexes = mutableMapOf<NeighborhoodSelection, Int>()
        /*
         * While it would be more efficient to group by entity set type and query all at once, filters can vary
         * by element so instead we
         *
         * (1) Create an unlogged table that's trimmed down to the relevant edges.
         * (2) For each neighborhood selection we build out the intermediate join tables
         * (3) Inner join intermediate join tables on initial pool of ids
         *
         * We can join to table :)
         *
         */

        // Join A to B
        // How do you achieve computing intersections that matter
        val connection = hds.connection

        query.selections.forEach {

        }

        TODO("hackathon")
    }

    override fun getQuery(queryId: UUID): GraphQuery {
        TODO("hackathon")

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
        val visitor = EntityQueryExecutingVisitor(hds, edm, authorizationManager, byteBlobDataManager, queryId)
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
        TODO("hackathon")

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

    private fun buildSrcJoinSql(
            incomingEntitySetIds: Set<UUID>,
            outgoingEntitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): String {
        val idsClause = "${EDGE_COMP_1.name} = ANY(?)"

        val srcEntitySetIdsClause = if (incomingEntitySetIds.isEmpty()) {
            ""
        } else {
            " AND ${SRC_ENTITY_SET_ID.name} IN (" + incomingEntitySetIds.joinToString(",") { id -> "'$id'" } + ")"
        }

        val edgeEntitySetIdsClause = if (associationEntitySetIds.isEmpty()) {
            ""
        } else {
            " AND ${EDGE_ENTITY_SET_ID.name} IN (" + associationEntitySetIds.joinToString(",") { id -> "'$id'" } + ")"
        }

        //For this there is no dstEntitySet clause since the target is self.
        return "SELECT * FROM ${EDGES.name} WHERE $idsClause $srcEntitySetIdsClause AND ${COMPONENT_TYPES.name} = ${ComponentType.SRC.ordinal}"
    }

    private fun buildDstJoinSql(
            incomingEntitySetIds: Set<UUID>,
            outgoingEntitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): String {
        val idsClause = "${EDGE_COMP_2.name} = ANY(?)"

        val dstEntitySetIdsClause = if (outgoingEntitySetIds.isEmpty()) {
            ""
        } else {
            " AND ${DST_ENTITY_SET_ID.name} = (" + outgoingEntitySetIds.joinToString(",") { id -> "'$id'" } + ")"
        }

        val edgeEntitySetIdsClause = if (associationEntitySetIds.isEmpty()) {
            ""
        } else {
            " AND ${EDGE_COMP_2.name} IN (" + associationEntitySetIds.joinToString(",") { id -> "'$id'" } + ")"
        }
        

        return "SELECT * FROM ${EDGES.name} WHERE $idsClause $dstEntitySetIdsClause AND ${COMPONENT_TYPES.name} = ${ComponentType.DST.ordinal}"
    }

    private fun buildEdgeJoinSql(
            incomingEntitySetIds: Set<UUID>,
            outgoingEntitySetIds: Set<UUID>,
            associationEntitySetIds: Set<UUID>
    ): String {
        val idsClause = "${EDGE_COMP_1.name} = ANY(?) OR ${EDGE_COMP_2.name} = ANY(?)"
        val edgeEntitySetIdsClause = if (associationEntitySetIds.isEmpty()) {
            ""
        } else {
            " AND ${SRC_ENTITY_SET_ID.name} IN (" + associationEntitySetIds.joinToString(",") { id -> "'$id'" } + ")"
        }

        return "SELECT * FROM ${EDGES.name} WHERE $idsClause $edgeEntitySetIdsClause AND ${COMPONENT_TYPES.name} = ${ComponentType.EDGE.ordinal}"
    }

    private fun buildWithClause(selection: NeighborhoodSelection): String {
        val incomingEntitySetIds = selection.incomingEntitySetIds.orElse(emptySet()) +
                getEntitySets((selection.incomingEntityTypeIds))
        val outgoingEntitySetIds = selection.outgingEntitySetIds.orElse(emptySet()) +
                getEntitySets(selection.outgingEntityTypeIds)
    }


    private fun getEntitySets(entityTypeIds: Optional<Set<UUID>>): List<UUID> {
        return entityTypeIds
                .map(edm::getEntitySetsOfType)
                .orElseGet { emptyList() }
                .map(EntitySet::getId)
    }
}

private val idsClause = "${PostgresColumn.ID_VALUE.name} IN ("
private val entitySetIdsClause = "${PostgresColumn.ENTITY_SET_ID.name} IN ("

internal fun createTemporaryTableSql(
        tableId: Int,
        entitySetIds: Set<UUID>,
        propertyTypeFqns: Map<UUID, String>,
        propertyTypes: Map<UUID, PropertyType>,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        propertyTypeFilters:
): String {
    val sql = selectEntitySetWithCurrentVersionOfPropertyTypes(
            entitySetIds.associateWith { Optional.empty<Set<UUID>>() },
            propertyTypeFqns,
            setOf(),
            authorizedPropertyTypes.mapValues { it.value.keys },
            propertyTypeFilters,
            EnumSet.noneOf(MetadataOption::class.java),
            propertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
            linking = false,
            omitEntitySetId = true
    )

    return "CREATE TEMPORARY VIEW temp_$tableId AS $sql"
}

const val TTL_MILLIS = 10 * 60 * 1000
//private val getQuerySql = "SELECT ${QUERY.name} FROM ${GRAPH_QUERIES.name} WHERE ${QUERY_ID.name} = ?"
private val readGraphQueryState = "SELECT * FROM ${GRAPH_QUERIES.name} WHERE ${QUERY_ID.name} = ?"
//private val insertGraphQuery =
//        "INSERT INTO ${GRAPH_QUERIES.name} (${QUERY_ID.name},${QUERY.name},${STATE.name},${START_TIME.name}) " +
//                "VALUES (?,?,?,?)"