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

package com.openlattice.graph.core

import com.google.common.collect.ImmutableList
import com.google.common.collect.SetMultimap
import com.openlattice.data.analytics.IncrementableWeightId
import com.openlattice.datastore.services.EdmService
import com.openlattice.graph.core.objects.NeighborTripletSet
import com.openlattice.graph.edge.Edge
import com.openlattice.graph.edge.EdgeKey
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
class Graph(private val hds: HikariDataSource, private val edm: EdmService) : GraphApi {
    override fun getEdgesAsMap(keys: MutableSet<EdgeKey>?): MutableMap<EdgeKey, Edge> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createEdges(keys: MutableSet<EdgeKey>): Int {
        val connection = hds.getConnection()
        connection.use {
            val ps = connection.prepareStatement(UPSERT_SQL)
            val version = System.currentTimeMillis()
            val versions = PostgresArrays.createLongArray(connection, ImmutableList.of(version))
            keys.forEach {
                ps.setObject(1, it.src.entitySetId)
                ps.setObject(2, it.src.entityKeyId)
                ps.setObject(3, it.dst.entitySetId)
                ps.setObject(4, it.dst.entityKeyId)
                ps.setObject(5, it.edge.entitySetId)
                ps.setObject(6, it.edge.entityKeyId)
                ps.setLong(7, version)
                ps.setArray(8, versions)
                ps.setLong(9, version)
                ps.setLong(10, version)
                ps.addBatch()
            }
            return ps.executeBatch().sum()
        }
    }

    override fun clearEdges(keys: MutableSet<EdgeKey>): Int {
        val connection = hds.getConnection()
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
        return if( iter.hasNext() ) { iter.next() } else { null }
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

    override fun getEdgesAndNeighborsForVertex(vertexId: UUID?): Stream<Edge> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getEdgesAndNeighborsForVertices(vertexIds: Set<UUID>?): Stream<Edge> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun computeGraphAggregation(
            limit: Int, entitySetId: UUID?, srcFilters: SetMultimap<UUID, UUID>?, dstFilters: SetMultimap<UUID, UUID>?
    ): Array<IncrementableWeightId> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getNeighborEntitySets(entitySetId: UUID?): NeighborTripletSet {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }


}

private fun selectEdges(keys: Set<EdgeKey>): String {
    return SELECT_SQL + "(" +
            keys
                    .asSequence()
                    .map { "(${it.src.entitySetId},${it.src.entityKeyId},${it.dst.entitySetId},${it.dst.entityKeyId},${it.edge.entitySetId},${it.edge.entityKeyId})" }
                    .joinToString(",") + ")"
}

private val KEY_COLUMNS = setOf(
        SRC_ENTITY_KEY_ID,
        SRC_ENTITY_SET_ID,
        DST_ENTITY_SET_ID,
        DST_ENTITY_KEY_ID,
        EDGE_ENTITY_SET_ID,
        EDGE_ENTITY_KEY_ID
).map { it.name }.toSet()

private val INSERT_COLUMNS = setOf(
        SRC_ENTITY_KEY_ID,
        SRC_ENTITY_SET_ID,
        DST_ENTITY_SET_ID,
        DST_ENTITY_KEY_ID,
        EDGE_ENTITY_SET_ID,
        EDGE_ENTITY_KEY_ID,
        VERSION,
        VERSIONS
).map { it.name }.toSet()

private val SELECT_SQL = "SELECT * FROM ${EDGES.name} " +
        "WHERE (${KEY_COLUMNS.joinToString(",")}) IN ";
private val UPSERT_SQL = "INSERT INTO ${EDGES.name} (${INSERT_COLUMNS.joinToString(",")}) VALUES (?,?,?,?,?,?,?,?) " +
        "ON CONFLICT (${KEY_COLUMNS.joinToString(",")}) DO UPDATE SET version = ?, versions = versions || ?"

private val CLEAR_SQL = "UPDATE ${EDGES.name} SET version = ?, versions = versions || ? WHERE ${KEY_COLUMNS.joinToString(
        " = ? AND "
)} = ? "
private val DELETE_SQL = "DELETE FROM ${EDGES.name} WHERE ${KEY_COLUMNS.joinToString(" = ? AND ")} = ? "

private val TOP_SQL = "SELECT ${SRC_ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name}, count(*) FROM ${EDGES.name} " +
        "WHERE ${SRC_ENTITY_SET_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] ))  " +
        "AND ${DST_ENTITY_SET_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] )) " +
        "AND ${EDGE_ENTITY_SET_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] )) " +
        "GROUP BY (${SRC_ENTITY_SET_ID.name}, ${SRC }"