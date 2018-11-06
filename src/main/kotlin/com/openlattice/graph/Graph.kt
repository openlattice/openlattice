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
import com.openlattice.analysis.AuthorizedFilteredRanking
import com.openlattice.analysis.requests.WeightedRankingAggregation
import com.openlattice.data.EntityDataKey
import com.openlattice.data.analytics.IncrementableWeightId
import com.openlattice.data.storage.entityKeyIdColumns
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.edge.Edge
import com.openlattice.graph.edge.EdgeKey
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.EDGES
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Stream
import kotlin.streams.toList

/**
 *
 */

const val SELF_ENTITY_SET_ID = "self_entity_set_id"
const val SELF_ENTITY_KEY_ID = "self_entity_key_id"
private val BATCH_SIZE = 10000

private val logger = LoggerFactory.getLogger(Graph::class.java)

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

    override fun getEntitiesForDestination(srcEntitySetIds: List<UUID>, edgeEntitySetIds: List<UUID>, dstEntityKeyIds: Set<UUID>): PostgresIterable<EdgeKey> {
        val query = "SELECT ${SRC_ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name}, ${DST_ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name}, ${EDGE_ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} " +
                "FROM ${EDGES.name} WHERE " +
                "${SRC_ENTITY_SET_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] )) AND " +
                "${EDGE_ENTITY_SET_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] )) AND " +
                "${DST_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] )) " +
                "ORDER BY ${DST_ENTITY_KEY_ID.name} "

        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(query)
                    stmt.fetchSize = BATCH_SIZE
                    stmt.setArray(1, PostgresArrays.createUuidArray(connection, srcEntitySetIds.stream()))
                    stmt.setArray(2, PostgresArrays.createUuidArray(connection, edgeEntitySetIds.stream()))
                    stmt.setArray(3, PostgresArrays.createUuidArray(connection, dstEntityKeyIds.stream()))
                    val rs = stmt.executeQuery()
                    logger.info(stmt.toString())
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EdgeKey> { ResultSetAdapters.edgeKey(it) }
        )
    }

    override fun getEdgeKeysContainingEntity(entitySetId: UUID, entityKeyId: UUID): Iterable<EdgeKey> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(NEIGHBORHOOD_SQL)
                    stmt.setObject(1, entitySetId)
                    stmt.setObject(2, entityKeyId)
                    stmt.setObject(3, entitySetId)
                    stmt.setObject(4, entityKeyId)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EdgeKey> { ResultSetAdapters.edgeKey(it) }
        )
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

    override fun getEdgesAndNeighborsForVerticesBulk(entitySetIds: Set<UUID>, vertexIds: Set<UUID>): Stream<Edge> {
        if(entitySetIds.size == 1) {
            return getEdgesAndNeighborsForVertices( entitySetIds.first(), vertexIds)
        }
        return PostgresIterable(
                Supplier {
                    val connection = hds.getConnection()
                    val ids = PostgresArrays.createUuidArray(connection, vertexIds.stream())
                    val stmt = connection.prepareStatement(BULK_BULK_NEIGHBORHOOD_SQL)
                    stmt.setObject(1, entitySetIds)
                    stmt.setArray(2, ids)
                    stmt.setObject(3, entitySetIds)
                    stmt.setArray(4, ids)
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Edge> { ResultSetAdapters.edge(it) }
        ).stream()
    }


    /**
     * 1. Compute the table of all neighbors of all relevant neighbors to person entity sets.
     * 2. Apply relevant filters for person entity sets with an inner join
     * 3. Apply relevant filters for associations with an innner join.
     *
     */
    override fun computeTopEntities(
            limit: Int,
            entitySetIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            filteredRankings: List<AuthorizedFilteredRanking>,
            linked: Boolean
    ): PostgresIterable<Map<String, Any>> {
        /*
         * The plan is that there are set of association entity sets and either source or destination entity sets.
         *
         * The plan is to do inner joins (ANDS) of property tables that are ORs of the filters and join them in to
         *
         * Join all association queries using full outer joins
         * Join all entity set queries using full outer joins
         *
         * The plan is to do all the aggregations in subqueries and then do outer joins on (entity_set_id, entity_key_id)
         * or linking_id while computing the score as the weighted sum of aggregates.
         *
         * For each association type we will query all entity sets of that type at once resulting in a table:
         *
         * | person_ek | edge_ek | 1_assoc_ptId | 2_assoc_pt_id | ...
         *
         * For each neighbor entity type we will query all entity sets of that type at once resulting in the table:
         *
         * | person_ek | other_ek | 1_assoc_ptId | 2_assoc_pt_id |
         *
         * The final table will be produced by doing a group by on the entity key or linking id resulting in a table:
         *
         * | person_ek | 1_assoc_ptId_agg | 2_assoc_pt_id_agg | 1_entity_ptId_agg | 2_entity_ptId_agg | score
         */

        val idColumns = if (linked) {
            listOf(LINKING_ID.name to EdmPrimitiveTypeKind.Guid)
        } else {
            listOf(SELF_ENTITY_SET_ID to EdmPrimitiveTypeKind.Guid, SELF_ENTITY_KEY_ID to EdmPrimitiveTypeKind.Guid)
        }

        val joinColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"

        val associationColumns = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            authorizedFilteredRanking.filteredRanking.associationAggregations.map {
                val column = aggregationAssociationColumnName(index, it.key)
                "${it.value.weight}*COALESCE($column,0)"
            }
        }.flatten()

        val entityColumns = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            authorizedFilteredRanking.filteredRanking.neighborTypeAggregations.map {
                val column = aggregationEntityColumnName(index, it.key)
                "${it.value.weight}*COALESCE($column,0)"
            }
        }.flatten()


        val countColumns = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            authorizedFilteredRanking.filteredRanking.countWeight.map {
                "$it*COALESCE(${associationCountColumnName(
                        index
                )},0)"
            }
                    .orElse("")
        }.filter(String::isNotBlank)

        val aggregationColumns = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            buildAggregationColumnMap(
                    index,
                    authorizedFilteredRanking.associationPropertyTypes,
                    authorizedFilteredRanking.filteredRanking.associationAggregations,
                    ASSOC
            ).map { it.value to authorizedFilteredRanking.associationPropertyTypes[it.key]!!.datatype } +
                    buildAggregationColumnMap(
                            index,
                            authorizedFilteredRanking.entitySetPropertyTypes,
                            authorizedFilteredRanking.filteredRanking.neighborTypeAggregations,
                            ENTITY
                    ).map { it.value to authorizedFilteredRanking.entitySetPropertyTypes[it.key]!!.datatype } +
                    (associationCountColumnName(index) to EdmPrimitiveTypeKind.Int64 )+
                    (entityCountColumnName(index) to EdmPrimitiveTypeKind.Int64)
        }.flatten().plus(idColumns).plus(SCORE.name to EdmPrimitiveTypeKind.Double).toMap()


        val scoreColumn = (associationColumns + entityColumns + countColumns)
                .joinToString("+", prefix = "(", postfix = ") as score")
        /*
         * Build the SQL for the association joins.
         */
        val associationSql =
                filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
                    val tableSql = buildAssociationTable(index, entitySetIds, authorizedFilteredRanking, linked)
                    //We are guaranteed at least one association for a valid top utilizers request
                    if (index == 0) {
                        "SELECT *,$scoreColumn FROM ($tableSql) as assoc_table$index "
                    } else {
                        "FULL OUTER JOIN ($tableSql) as assoc_table$index USING($joinColumns) "
                    }
                }.joinToString("\n")

        val entitiesSql = filteredRankings.mapIndexed { index, authorizedFilteredRanking ->
            val tableSql = buildEntityTable(index, entitySetIds, authorizedFilteredRanking, linked)
            "FULL OUTER JOIN ($tableSql) as entity_table$index USING($joinColumns) "
        }.joinToString("\n")
        val sql = "$associationSql \n$entitiesSql \nORDER BY score DESC \nLIMIT $limit"

        return PostgresIterable(
                Supplier {
                    val connnection = hds.connection
                    val stmt = connnection.createStatement()
                    val rs = stmt.executeQuery(sql)
                    StatementHolder(connnection, stmt, rs)
                }, Function { rs ->
            return@Function aggregationColumns.map { (col, type) ->
                when (type) {
                    EdmPrimitiveTypeKind.Guid -> col to (rs.getObject(col) as UUID)
                    else -> col to rs.getObject(col)
                }
            }.toMap()
        })
    }


    override fun computeGraphAggregation(
            limit: Int, entitySetId: UUID, srcFilters: SetMultimap<UUID, UUID>, dstFilters: SetMultimap<UUID, UUID>
    ): Array<IncrementableWeightId> {
        return topEntitiesOld(limit, entitySetId, srcFilters, dstFilters).toList().toTypedArray()
    }

    override fun topEntitiesOld(
            limit: Int, entitySetId: UUID, srcFilters: SetMultimap<UUID, UUID>, dstFilters: SetMultimap<UUID, UUID>
    ): Stream<IncrementableWeightId> {
        return topEntitiesWorker(limit, entitySetId, srcFilters, dstFilters).map {
            IncrementableWeightId(
                    it.first.entityKeyId, it.second
            )
        }
    }

    private fun topEntitiesWorker(
            limit: Int, entitySetId: UUID, srcFilters: SetMultimap<UUID, UUID>, dstFilters: SetMultimap<UUID, UUID>
    ): Stream<Pair<EntityDataKey, Long>> {
        val countColumn = "total_count"
        val query = getTopUtilizersSql(entitySetId, srcFilters, dstFilters, limit)
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.createStatement()
                    logger.info("Executing top utilizer query: {}", query)
                    val rs = stmt.executeQuery(query)
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Pair<EntityDataKey, Long>> {
                    ResultSetAdapters.entityDataKey(it) to it.getLong(countColumn)
                }
        ).stream()
    }

    override fun getNeighborEntitySets(entitySetIds: Set<UUID>): List<NeighborSets> {
        val neighbors: MutableList<NeighborSets> = ArrayList()

        val query = "SELECT DISTINCT ${SRC_ENTITY_SET_ID.name},${EDGE_ENTITY_SET_ID.name}, ${DST_ENTITY_SET_ID.name} " +
                "FROM ${EDGES.name} " +
                "WHERE ${SRC_ENTITY_SET_ID.name} = ANY(?) OR ${DST_ENTITY_SET_ID.name} = ANY(?)"
        val connection = hds.connection
        connection.use {
            val ps = connection.prepareStatement(query)
            ps.use {
                val entitySetIdsArr = PostgresArrays.createUuidArray(connection, entitySetIds)
                ps.setObject(1, entitySetIdsArr)
                ps.setObject(2, entitySetIdsArr)
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


    private fun buildAssociationTable(
            index: Int,
            selfEntitySetIds: Set<UUID>,
            authorizedFilteredRanking: AuthorizedFilteredRanking,
            linked: Boolean
    ): String {
        val esEntityKeyIds = edm
                .getEntitySetsOfType(authorizedFilteredRanking.filteredRanking.associationTypeId)
                .map { it.id to Optional.empty<Set<UUID>>() }
                .toMap()
        val associationPropertyTypes = authorizedFilteredRanking.associationPropertyTypes
        //This will read all the entity sets of the same type all at once applying filters.
        val dataSql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                esEntityKeyIds,
                associationPropertyTypes.mapValues { quote(it.value.type.fullQualifiedNameAsString) },
                authorizedFilteredRanking.filteredRanking.associationAggregations.keys,
                authorizedFilteredRanking.associationSets,
                authorizedFilteredRanking.filteredRanking.associationFilters,
                setOf(),
                linked,
                associationPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary }
        )

        val baseEntityColumnsSql = if (authorizedFilteredRanking.filteredRanking.dst) {
            "${SRC_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $SELF_ENTITY_KEY_ID, " +
                    "${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}"
        } else {
            "${DST_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $SELF_ENTITY_KEY_ID, " +
            "${EDGE_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${EDGE_ENTITY_KEY_ID.name} as ${ID_VALUE.name}"
        }

        val edgeClause = buildEdgeFilteringClause(selfEntitySetIds, authorizedFilteredRanking)
        val joinColumns = if (linked) LINKING_ID.name else entityKeyIdColumns
        val groupingColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"
        val idSql = "SELECT ${ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${ID.name} as $SELF_ENTITY_KEY_ID FROM ${IDS.name}, ${LINKING_ID.name}"
        val spineSql = if (linked) {
            "SELECT edges.*,${LINKING_ID.name} FROM (SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause) as edges " +
                    "LEFT JOIN ($idSql) as ${IDS.name} USING ($SELF_ENTITY_SET_ID,$SELF_ENTITY_KEY_ID)"
        } else {
            "SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause"
        }

        val aggregationColumns =
                authorizedFilteredRanking.filteredRanking.associationAggregations
                        .mapValues {
                            val fqn = quote(associationPropertyTypes[it.key]!!.type.fullQualifiedNameAsString)
                            val alias = aggregationAssociationColumnName(index, it.key)
                            "${it.value.type.name}(COALESCE($fqn[1],0)) as $alias"
                        }.values.joinToString(",")
        val countAlias = associationCountColumnName(index)
        val allColumns = listOf(groupingColumns, aggregationColumns, "count(*) as $countAlias")
                .filter(String::isNotBlank)
                .joinToString(",")
        return "SELECT $allColumns " +
                "FROM ($spineSql) as spine INNER JOIN ($dataSql) as data USING($joinColumns) " +
                "GROUP BY ($groupingColumns)"
    }


    private fun buildEntityTable(
            index: Int,
            selfEntitySetIds: Set<UUID>,
            authorizedFilteredRanking: AuthorizedFilteredRanking,
            linked: Boolean
    ): String {
        val esEntityKeyIds = edm
                .getEntitySetsOfType(authorizedFilteredRanking.filteredRanking.neighborTypeId)
                .map { it.id to Optional.empty<Set<UUID>>() }
                .toMap()
        val entitySetPropertyTypes = authorizedFilteredRanking.entitySetPropertyTypes
        //This will read all the entity sets of the same type all at once applying filters.
        val dataSql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                esEntityKeyIds,
                entitySetPropertyTypes.mapValues { quote(it.value.type.fullQualifiedNameAsString) },
                authorizedFilteredRanking.filteredRanking.neighborTypeAggregations.keys,
                authorizedFilteredRanking.entitySets,
                authorizedFilteredRanking.filteredRanking.neighborFilters,
                setOf(),
                linked,
                entitySetPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary }
        )

        val baseEntityColumnsSql = if (authorizedFilteredRanking.filteredRanking.dst) {
            "${SRC_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${SRC_ENTITY_KEY_ID.name} as $SELF_ENTITY_KEY_ID, " +
                    "${DST_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} as ${ID_VALUE.name}"
        } else {
            "${DST_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${DST_ENTITY_KEY_ID.name} as $SELF_ENTITY_KEY_ID, " +
            "${SRC_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name} as ${ID_VALUE.name}"
        }

        val edgeClause = buildEdgeFilteringClause(selfEntitySetIds, authorizedFilteredRanking)
        val joinColumns = if (linked) LINKING_ID.name else entityKeyIdColumns
        val groupingColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"
        val idSql = "SELECT ${ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${ID.name} as $SELF_ENTITY_KEY_ID FROM ${IDS.name}, ${LINKING_ID.name}"
        val spineSql = if (linked) {
            "SELECT edges.*,${LINKING_ID.name} FROM (SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause) as edges " +
                    "LEFT JOIN ($idSql) as ${IDS.name} USING ($SELF_ENTITY_SET_ID,$SELF_ENTITY_KEY_ID)"
        } else {
            "SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause"
        }

        val aggregationColumns =
                authorizedFilteredRanking.filteredRanking.neighborTypeAggregations
                        .mapValues {
                            val fqn = quote(entitySetPropertyTypes[it.key]!!.type.fullQualifiedNameAsString)
                            val alias = aggregationEntityColumnName(index, it.key)
                            "${it.value.type.name}(COALESCE($fqn[1],0)) as $alias"
                        }.values.joinToString(",")
        val countAlias = entityCountColumnName(index)
        val allColumns = listOf(groupingColumns, aggregationColumns, "count(*) as $countAlias")
                .filter(String::isNotBlank)
                .joinToString(",")
        return "SELECT $allColumns " +
                "FROM ($spineSql) as spine INNER JOIN ($dataSql) as data USING($joinColumns) " +
                "GROUP BY ($groupingColumns)"
    }
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

/**
 * Builds the SQL query for top utilizers.
 *
 * filter keys are allowed association entity set ids
 *
 * filter values are allowed neighbor entity set ids
 *
 * The simple version of top utilizers should count the number of allowed neighbors.
 *
 * That is for each
 */
internal fun getTopUtilizersSql(
        entitySetId: UUID,
        srcFilters: SetMultimap<UUID, UUID>,
        dstFilters: SetMultimap<UUID, UUID>,
        top: Int = 100
): String {

    return "SELECT ${ENTITY_SET_ID.name}, ${ID_VALUE.name}, (COALESCE(src_count,0) + COALESCE(dst_count,0)) as total_count " +
            "FROM (${getTopUtilizersFromSrc(entitySetId, srcFilters)}) as src_counts " +
            "FULL OUTER JOIN (${getTopUtilizersFromDst(entitySetId, dstFilters)}) as dst_counts " +
            "USING(${ENTITY_SET_ID.name}, ${ID_VALUE.name}) " +
//            "WHERE total_count IS NOT NULL " +
            "ORDER BY total_count DESC " +
            "LIMIT $top"

}

internal fun getTopUtilizersFromSrc(entitySetId: UUID, filters: SetMultimap<UUID, UUID>): String {
    val countColumn = "src_count"
    return "SELECT ${SRC_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, count(*) as $countColumn " +
            "FROM EDGES WHERE ${srcClauses(entitySetId, filters)} " +
            "GROUP BY (${ENTITY_SET_ID.name}, ${ID_VALUE.name}) "
}

internal fun getTopUtilizersFromDst(entitySetId: UUID, filters: SetMultimap<UUID, UUID>): String {
    val countColumn = "dst_count"
    return "SELECT ${DST_ENTITY_SET_ID.name} as ${ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, count(*) as $countColumn " +
            "FROM EDGES WHERE ${dstClauses(entitySetId, filters)} " +
            "GROUP BY (${ENTITY_SET_ID.name}, ${ID_VALUE.name}) "
}

private val SELECT_SQL = "SELECT * FROM ${EDGES.name} " +
        "WHERE (${KEY_COLUMNS.joinToString(",")}) IN "
private val UPSERT_SQL = "INSERT INTO ${EDGES.name} (${INSERT_COLUMNS.joinToString(",")}) VALUES (?,?,?,?,?,?,?,?) " +
        "ON CONFLICT (${KEY_COLUMNS.joinToString(",")}) " +
        "DO UPDATE SET version = EXCLUDED.version, versions = ${EDGES.name}.versions || EXCLUDED.version"

private val CLEAR_SQL = "UPDATE ${EDGES.name} SET version = ?, versions = versions || ? " +
        "WHERE ${KEY_COLUMNS.joinToString(" = ? AND ")} = ? "
private val DELETE_SQL = "DELETE FROM ${EDGES.name} WHERE ${KEY_COLUMNS.joinToString(" = ? AND ")} = ? "

private val NEIGHBORHOOD_SQL = "SELECT * FROM ${EDGES.name} WHERE " +
        "(${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ?) OR " +
        "(${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ?)"

private val BULK_NEIGHBORHOOD_SQL = "SELECT * FROM ${EDGES.name} WHERE " +
        "(${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] ))) OR " +
        "(${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] )))"

private val BULK_BULK_NEIGHBORHOOD_SQL = "SELECT * FROM ${EDGES.name} WHERE " +
        "( ${SRC_ENTITY_SET_ID.name} IN ( SELECT * FROM UNNEST( (?)::uuid[] ) ) AND ${SRC_ENTITY_KEY_ID.name} IN ( SELECT * FROM UNNEST( (?)::uuid[] ) ) ) OR " +
        "( ${DST_ENTITY_SET_ID.name} IN ( SELECT * FROM UNNEST( (?)::uuid[] ) ) AND ${DST_ENTITY_KEY_ID.name} IN ( SELECT * FROM UNNEST( (?)::uuid[] )) )"


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

private fun buildEdgeFilteringClause(
        selfEntitySetIds: Set<UUID>,
        authorizedFilteredRanking: AuthorizedFilteredRanking
): String {
    val authorizedAssociationEntitySets = authorizedFilteredRanking.associationSets.keys
    val authorizedEntitySets = authorizedFilteredRanking.entitySets.keys

    val associationsClause =
            "${EDGE_ENTITY_SET_ID.name} IN (${authorizedAssociationEntitySets.joinToString(",") { "'$it'" }})"

    val entitySetColumn = if (authorizedFilteredRanking.filteredRanking.dst) {
        DST_ENTITY_SET_ID.name
    } else {
        SRC_ENTITY_SET_ID.name
    }

    val selfEntitySetColumn = if (authorizedFilteredRanking.filteredRanking.dst) {
        SRC_ENTITY_SET_ID.name
    } else {
        DST_ENTITY_SET_ID.name
    }

    val entitySetsClause =
        "$entitySetColumn IN (${authorizedEntitySets.joinToString(",") { "'$it'" }}) " +
                "AND $selfEntitySetColumn IN (${selfEntitySetIds.joinToString(",") { "'$it'" }})"

    return "($associationsClause AND $entitySetsClause)"
}

/**
 * Generates an association clause for querying the edges table.
 * @param entitySetColumn The column to use for filtering allowed entity set ids.
 * @param entitySetId The entity set id for which the aggregation will be performed.
 * @param neighborColumn The column for the neighbors that will be counted.
 * @param associationFilters A multimap from association entity set ids to allowed neighbor entity set ids.
 */
private fun associationClauses(
        entitySetColumn: String, entitySetId: UUID, neighborColumn: String, associationFilters: SetMultimap<UUID, UUID>
): String {
    if (associationFilters.isEmpty) {
        return " false "
    }
    return Multimaps
            .asMap(associationFilters)
            .asSequence()
            .map {
                "($entitySetColumn = '$entitySetId' AND ${EDGE_ENTITY_SET_ID.name} = '${it.key}' " +
                        "AND $neighborColumn IN (${it.value.joinToString(",") { "'$it'" }}) ) "
            }
            .joinToString(" OR ")
}

const val ASSOC = "assoc"
const val ENTITY = "entity"
internal fun buildAggregationColumnMap(
        index: Int,
        propertyTypes: Map<UUID, PropertyType>,
        aggregations: Map<UUID, WeightedRankingAggregation>,
        type: String
): Map<UUID, String> {
    return aggregations
            .mapValues {

                when (type) {
                    ASSOC -> aggregationAssociationColumnName(index, it.key).replace("\"", "")
                    ENTITY -> aggregationEntityColumnName(index, it.key).replace("\"", "")
                    else -> throw IllegalStateException("Unsupported aggregation type: $type.")
                }
            }
}

internal fun aggregationAssociationColumnName(index: Int, propertyTypeId: UUID): String {
    return quote("${ASSOC}_${index}_$propertyTypeId")
}

internal fun aggregationEntityColumnName(index: Int, propertyTypeId: UUID): String {
    return quote("${ENTITY}_${index}_$propertyTypeId")
}

internal fun entityCountColumnName(index: Int): String {
    return "${ENTITY}_${index}_count"
}

internal fun associationCountColumnName(index: Int): String {
    return "${ASSOC}_${index}_count"
}