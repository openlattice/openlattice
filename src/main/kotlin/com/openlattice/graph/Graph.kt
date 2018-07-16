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

import com.google.common.collect.ImmutableList
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.data.EntityDataKey
import com.openlattice.data.analytics.IncrementableWeightId
import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.edge.Edge
import com.openlattice.graph.edge.EdgeKey
import com.openlattice.postgres.DataTables.COUNT_FQN
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.EDGES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class Graph(private val hds: HikariDataSource, private val edm: EdmManager) : GraphService {

    override fun getEdgesAsMap(keys: MutableSet<EdgeKey>?): MutableMap<EdgeKey, Edge> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createEdges(keys: MutableSet<EdgeKey>): Int {
        hds.connection.use {
            val ps = it.prepareStatement(UPSERT_SQL)
            val version = System.currentTimeMillis()
            val versions = PostgresArrays.createLongArray(it, ImmutableList.of(version))
            ps.use {
                keys.forEach {
                    ps.setObject(1, it.src.entitySetId)
                    ps.setObject(2, it.src.entityKeyId)
                    ps.setObject(3, it.dst.entitySetId)
                    ps.setObject(4, it.dst.entityKeyId)
                    ps.setObject(5, it.edge.entitySetId)
                    ps.setObject(6, it.edge.entityKeyId)
                    ps.setLong(7, version)
                    ps.setArray(8, versions)
                    ps.addBatch()
                }
                return ps.executeBatch().sum()
            }
        }
    }

    override fun clearEdges(keys: MutableSet<EdgeKey>): Int {
        val connection = hds.connection
        connection.use {
            val ps = connection.prepareStatement(CLEAR_SQL)
            val version = -System.currentTimeMillis()
            keys.forEach {
                ps.setLong(1, version)
                ps.setLong(2, version)
                ps.setObject(3, it.src.entitySetId)
                ps.setObject(4, it.src.entityKeyId)
                ps.setObject(5, it.dst.entitySetId)
                ps.setObject(6, it.dst.entityKeyId)
                ps.setObject(7, it.edge.entitySetId)
                ps.setObject(8, it.edge.entityKeyId)
                ps.addBatch()
            }
            return ps.executeBatch().sum()
        }
    }

    override fun clearVerticesInEntitySet(entitySetId: UUID?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun clearVertices(entitySetId: UUID?, vertices: Set<UUID>?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deleteEdges(keys: MutableSet<EdgeKey>?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deleteVerticesInEntitySet(entitySetId: UUID?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deleteVertices(entitySetId: UUID?, vertices: MutableSet<UUID>?): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getEdge(key: EdgeKey): Edge? {
        val iter = getEdges(setOf(key)).iterator()
        return if (iter.hasNext()) {
            iter.next()
        } else {
            null
        }
    }

    override fun getEdges(keys: Set<EdgeKey>): Stream<Edge> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.getConnection()
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(selectEdges(keys))
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }

    fun executeEdgesQuery(query: String): Stream<Edge> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.getConnection()
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(query)
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }

    override fun getEdgesAndNeighborsForVertex(entitySetId: UUID, vertexId: UUID): Stream<Edge> {

        return PostgresIterable(
                Supplier {
                    val connection = hds.getConnection()
                    val stmt = connection.prepareStatement(NEIGHBORHOOD_SQL)
                    stmt.setObject(1, entitySetId)
                    stmt.setObject(2, vertexId)
                    stmt.setObject(3, entitySetId)
                    stmt.setObject(4, vertexId)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }

    override fun getEdgesAndNeighborsForVertices(entitySetId: UUID, vertexIds: Set<UUID>): Stream<Edge> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.getConnection()
                    val ids = PostgresArrays.createUuidArray(connection, vertexIds.stream())
                    val stmt = connection.prepareStatement(BULK_NEIGHBORHOOD_SQL)
                    stmt.setObject(1, entitySetId)
                    stmt.setArray(2, ids)
                    stmt.setObject(3, entitySetId)
                    stmt.setArray(4, ids)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }

    override fun computeGraphAggregation(
            limit: Int, entitySetId: UUID?, srcFilters: SetMultimap<UUID, UUID>?, dstFilters: SetMultimap<UUID, UUID>?
    ): Array<IncrementableWeightId> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun topEntities(
            limit: Int, entitySetId: UUID, srcFilters: SetMultimap<UUID, UUID>, dstFilters: SetMultimap<UUID, UUID>
    ): Stream<EntityDataKey> {
        val countColumn = quote(COUNT_FQN.fullQualifiedNameAsString)
        val query = "SELECT ${ENTITY_SET_ID.name}, " +
                "${caseExpression(entitySetId)} as ${ID_VALUE.name}, " +
                "count(*) as $countColumn " +
                "FROM edges " +
                "WHERE ${srcClauses(entitySetId, srcFilters)} " +
                "AND ${dstClauses(entitySetId, dstFilters)}" +
                "GROUP BY (${ENTITY_SET_ID.name}, ${ID_VALUE.name}) " +
                "ORDER BY $countColumn DESC"
        return PostgresIterable(
                Supplier {
                    val connection = hds.getConnection()
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(query)
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EntityDataKey> { ResultSetAdapters.entityDataKey(it) }
        ).stream()
    }

    override fun getNeighborEntitySets(entitySetId: UUID?): List<NeighborSets> {
        val neighbors: MutableList<NeighborSets> = ArrayList()
        val query = "SELECT DISTINCT(${SRC_ENTITY_SET_ID.name},${EDGE_ENTITY_SET_ID.name} ${DST_ENTITY_SET_ID.name}) " +
                "FROM ${EDGES.name} " +
                "WHERE ${SRC_ENTITY_SET_ID.name} = ? OR ${DST_ENTITY_SET_ID.name} = ?"
        val connection = hds.getConnection()
        connection.use {
            val ps = connection.prepareStatement(query)
            ps.use {
                ps.setObject(1, entitySetId)
                ps.setObject(2, entitySetId)
                val rs = ps.executeQuery()
                while (rs.next()) {
                    val srcEntitySetId = rs.getObject(SRC_ENTITY_SET_ID.name) as UUID
                    val edgeEntitySetId = rs.getObject(EDGE_ENTITY_SET_ID.name) as UUID
                    val dstEntitySetId = rs.getObject(DST_ENTITY_SET_ID.name) as UUID
                    neighbors.add(
                            NeighborSets(srcEntitySetId, edgeEntitySetId, dstEntitySetId)
                    )
                }
            }
        }
        return neighbors
    }
}

private fun selectEdges(keys: Set<EdgeKey>): String {
    return "$SELECT_SQL(" +
            keys
                    .asSequence()
                    .map { "(${it.src.entitySetId},${it.src.entityKeyId},${it.dst.entitySetId},${it.dst.entityKeyId},${it.edge.entitySetId},${it.edge.entityKeyId})" }
                    .joinToString(",") + ")"
}

private fun srcClauses(entitySetId: UUID, associationFilters: SetMultimap<UUID, UUID>): String {
    return associationClauses(
            SRC_ENTITY_SET_ID.name, entitySetId, DST_ENTITY_SET_ID.name, associationFilters
    )
}

private fun dstClauses(entitySetId: UUID, associationFilters: SetMultimap<UUID, UUID>): String {
    return associationClauses(
            DST_ENTITY_SET_ID.name, entitySetId, SRC_ENTITY_SET_ID.name, associationFilters
    )
}

/**
 * Generates an association clause for querying the edges table.
 * @param entitySetColumn The column to use for filtering allowed entity set ids.
 * @param entitySetId The entity set id for which the aggregation will be performed.
 * @param neighborColumn The column for the neighbors that will be counted.
 * @param associationFilters A multimap from association entity set ids to allowed entity set ids.
 */
private fun associationClauses(
        entitySetColumn: String, entitySetId: UUID, neighborColumn: String, associationFilters: SetMultimap<UUID, UUID>
): String {
    return Multimaps
            .asMap(associationFilters)
            .asSequence()
            .map {
                "$entitySetColumn = $entitySetId AND ${EDGE_ENTITY_SET_ID.name} = ${it.key} " +
                        "$neighborColumn IN (${it.value.map { "'$it'" }.joinToString(",")}) "
            }
            .joinToString(",")
}

//TODO: Extract string constants.
private fun caseExpression(entitySetId: UUID): String {
    return " CASE WHEN ${SRC_ENTITY_SET_ID.name}=$entitySetId THEN ${SRC_ENTITY_KEY_ID.name} ELSE ${DST_ENTITY_KEY_ID.name} END "
}

private val KEY_COLUMNS = setOf(
        SRC_ENTITY_SET_ID,
        SRC_ENTITY_KEY_ID,
        DST_ENTITY_SET_ID,
        DST_ENTITY_KEY_ID,
        EDGE_ENTITY_SET_ID,
        EDGE_ENTITY_KEY_ID
).map { it.name }.toSet()

private val INSERT_COLUMNS = setOf(
        SRC_ENTITY_SET_ID,
        SRC_ENTITY_KEY_ID,
        DST_ENTITY_SET_ID,
        DST_ENTITY_KEY_ID,
        EDGE_ENTITY_SET_ID,
        EDGE_ENTITY_KEY_ID,
        VERSION,
        VERSIONS
).map { it.name }.toSet()

private val SELECT_SQL = "SELECT * FROM ${EDGES.name} " +
        "WHERE (${KEY_COLUMNS.joinToString(",")}) IN "
private val UPSERT_SQL = "INSERT INTO ${EDGES.name} (${INSERT_COLUMNS.joinToString(",")}) VALUES (?,?,?,?,?,?,?,?) " +
        "ON CONFLICT (${KEY_COLUMNS.joinToString(",")}) " +
        "DO UPDATE SET version = EXCLUDED.version, versions = ${EDGES.name}.versions || EXCLUDED.version"

private val CLEAR_SQL = "UPDATE ${EDGES.name} SET version = ?, versions = versions || ? " +
        "WHERE ${KEY_COLUMNS.joinToString(        " = ? AND ")} = ? "
private val DELETE_SQL = "DELETE FROM ${EDGES.name} WHERE ${KEY_COLUMNS.joinToString(" = ? AND ")} = ? "

private val NEIGHBORHOOD_SQL = "SELECT * FROM ${EDGES.name} WHERE " +
        "(${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ?) OR " +
        "(${DST_ENTITY_SET_ID.name} = ? AND $${DST_ENTITY_KEY_ID.name} = ?)"

private val BULK_NEIGHBORHOOD_SQL = "SELECT * FROM ${EDGES.name} WHERE " +
        "(${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] ))) OR " +
        "(${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] )))"

