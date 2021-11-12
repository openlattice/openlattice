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

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.annotation.Timed
import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.common.collect.ImmutableList
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking
import com.openlattice.analysis.requests.*
import com.openlattice.data.*
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService
import com.openlattice.data.storage.entityKeyIdColumns
import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.edge.Edge
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.E
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.PreparedStatement
import java.util.*
import java.util.stream.Stream

/**
 *
 */

const val SELF_ENTITY_SET_ID = "self_entity_set_id"
const val SELF_ENTITY_KEY_ID = "self_entity_key_id"
private const val BATCH_SIZE = 10_000

private val logger = LoggerFactory.getLogger(Graph::class.java)

/**
 * The object graph is triplicated onto src, dst, and edge entity sets on every write.
 */
@Service
class Graph(
        private val dataSourceResolver: DataSourceResolver,
        private val entitySetManager: EntitySetManager,
        private val pgDataQueryService: PostgresEntityDataQueryService,
        private val entityKeyIdService: EntityKeyIdService,
        private val metricRegistry: MetricRegistry
) : GraphService {

    /* Create */

    override fun createEdges(keys: Set<DataEdgeKey>): WriteEvent {
        val srcEntitySetEdgeKeys = keys.groupBy { it.src.entitySetId }
        val dstEntitySetEdgeKeys = keys.groupBy { it.dst.entitySetId }
        val edgeEntitySetEdgeKeys = keys.groupBy { it.edge.entitySetId }

        val version = System.currentTimeMillis()

        val numUpdated = createEdgesForDataSource(
                srcEntitySetEdgeKeys,
                version
        ) + createEdgesForDataSource(
                dstEntitySetEdgeKeys,
                version
        ) + createEdgesForDataSource(
                edgeEntitySetEdgeKeys,
                version
        ) //Return value not used at the moment, need to consider returning total number of writes.

        return WriteEvent(version, keys.size)
    }

    private fun createEdgesForDataSource(
            keyMap: Map<UUID, List<DataEdgeKey>>,
            version: Long
    ): Int {
        return keyMap.map { (entitySetId, keys) ->
            val hds = dataSourceResolver.resolve(entitySetId)
            hds.connection.use { connection ->
                val ps = connection.prepareStatement(EDGES_UPSERT_SQL)

                val versions = PostgresArrays.createLongArray(connection, ImmutableList.of(version))

                ps.use {
                    keys.forEach { key ->
                        bindColumnsForEdge(ps, key, version, versions)
                    }
                    ps.executeBatch().sum()
                }
            }
        }.sum()
    }


    private fun addKeyIds(ps: PreparedStatement, dataEdgeKey: DataEdgeKey, startIndex: Int = 1) {
        logger.info("Adding data edge key {}", dataEdgeKey)
        ps.setObject(startIndex + 1, dataEdgeKey.src.entityKeyId)
        ps.setObject(startIndex + 2, dataEdgeKey.dst.entityKeyId)
        ps.setObject(startIndex + 3, dataEdgeKey.edge.entityKeyId)
        ps.addBatch()
    }

    private fun lockAndOperateOnEdges(
            keys: Iterable<DataEdgeKey>,
            statement: String,
            statementSupplier: (lockStmt: PreparedStatement, operationStmt: PreparedStatement, dataEdgeKey: DataEdgeKey) -> Unit
    ): Int {
        val srcEntitySetEdgeKeys = keys.groupBy { it.src.entitySetId }
        val dstEntitySetEdgeKeys = keys.groupBy { it.dst.entitySetId }
        val edgeEntitySetEdgeKeys = keys.groupBy { it.edge.entitySetId }

        return lockAndOperateOnEdges(srcEntitySetEdgeKeys, statement, statementSupplier) +
                lockAndOperateOnEdges(dstEntitySetEdgeKeys, statement, statementSupplier) +
                lockAndOperateOnEdges(edgeEntitySetEdgeKeys, statement, statementSupplier)
    }

    private fun lockAndOperateOnEdges(
            keyMap: Map<UUID, List<DataEdgeKey>>,
            statement: String,
            statementSupplier: (lockStmt: PreparedStatement, operationStmt: PreparedStatement, dataEdgeKey: DataEdgeKey) -> Unit
    ): Int {
        return keyMap.map { (entitySetId, keys) ->
            val hds = dataSourceResolver.resolve(entitySetId)
            hds.connection.use { connection ->
                connection.autoCommit = false
                val updates = connection.prepareStatement(LOCK_BY_VERTEX_SQL).use { psLocks ->
                    connection.prepareStatement(statement).use { psExecute ->
                        keys.forEach { dataEdgeKey ->
                            statementSupplier(psLocks, psExecute, dataEdgeKey)
                        }
                        psLocks.executeBatch()
                        psExecute.executeBatch().sum()
                    }
                }
                connection.commit()
                connection.autoCommit = true
                updates
            }
        }.sum()
    }

    private fun clearEdgesAddVersion(ps: PreparedStatement, version: Long) {
        ps.setLong(1, version)
        ps.setLong(2, version)
    }

    /* Delete  */

    @Deprecated("Redundant function call.", replaceWith = ReplaceWith("deleteEdges"))
    override fun clearEdges(keys: Iterable<DataEdgeKey>): Int {
        val version = -System.currentTimeMillis()
        return lockAndOperateOnEdges(keys, CLEAR_BY_VERTEX_SQL) { lockStmt, operationStmt, dataEdgeKey ->
            addKeyIds(lockStmt, dataEdgeKey)
            clearEdgesAddVersion(operationStmt, version)
            addKeyIds(operationStmt, dataEdgeKey, 3)
        }
    }

    override fun deleteEdges(keys: Iterable<DataEdgeKey>, deleteType: DeleteType): WriteEvent {
        val sql = when (deleteType) {
            DeleteType.Hard -> HARD_DELETE_EDGES_SQL
            DeleteType.Soft -> SOFT_DELETE_EDGES_SQL
        }
        val version = -System.currentTimeMillis()
        val updates = lockAndOperateOnEdges(keys, sql) { lockStmt, operationStmt, dataEdgeKey ->
            var opIndex = 1
            //For soft deletes we have to bind version twice
            if (deleteType == DeleteType.Soft) {
                operationStmt.setLong(opIndex++, version)
                operationStmt.setLong(opIndex++, version)
            }

            addKeyIds(lockStmt, dataEdgeKey)
            addKeyIds(operationStmt, dataEdgeKey, opIndex)
        }
        return WriteEvent(System.currentTimeMillis(), updates)
    }

    /* Select */

    override fun getEdgeKeysContainingEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            includeClearedEdges: Boolean
    ): BasePostgresIterable<DataEdgeKey> {
        val sql = if (includeClearedEdges) BULK_NEIGHBORHOOD_SQL else BULK_NON_TOMBSTONED_NEIGHBORHOOD_SQL
        val hds = dataSourceResolver.resolve(entitySetId)
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, sql, BATCH_SIZE, false) { ps ->
            val idArr = PostgresArrays.createUuidArray(ps.connection, entityKeyIds)
            ps.setArray(1, idArr)
            ps.setObject(2, entitySetId)
            ps.setArray(3, idArr)
            ps.setObject(4, entitySetId)
            ps.setArray(5, idArr)
            ps.setObject(6, entitySetId)
        }) {
            ResultSetAdapters.edgeKey(it)
        }
    }

    /**
     * @param entitySetIds The base entity set ids for the neighbor query
     * @param pagedNeighborRequest
     */
    @Timed
    override fun getEdgesAndNeighborsForVertices(
            entitySetIds: Set<UUID>,
            pagedNeighborRequest: PagedNeighborRequest
    ): Stream<Edge> {

        val filter = pagedNeighborRequest.filter

        val srcEntitySetIds = filter.srcEntitySetIds.orElse(setOf())
        val allEntitySetIds = srcEntitySetIds + entitySetIds

        /**
         * There seems to be a weird thing here where entitySetIds for which to find neighbors for are specified
         * in entitySetIds and in the pagedNeighborRequest.srcEntitySetIds. In theory srcEntitySetIds are a subset of
         * of entitySetIds based on the authorization called in the calling paths, but not all calling paths perform
         * that authorization. So the filtered neighborhood sql will have manually constructed srcEntitySetIds clauses,
         * but the bind parameters will use the separately provided entity set ids.
         *
         * Another note here is that we could filter down each query to make it smaller, but it's a simpler code
         * change for now to repeat the full query on all nodes.
         */
        val edges = entitySetIds
                .groupBy { dataSourceResolver.getDataSourceName(it) }
                .flatMap { (dataSourceName, entitySetIdsForDataSource) ->
                    BasePostgresIterable(
                            PreparedStatementHolderSupplier(
                                    dataSourceResolver.getDataSource(dataSourceName),
                                    getFilteredNeighborhoodSql(pagedNeighborRequest)
                            ) { ps ->
                                val connection = ps.connection
                                val idsArr = PostgresArrays.createUuidArray(connection, filter.entityKeyIds)
                                val entitySetIdsArr = PostgresArrays.createUuidArray(
                                        connection, entitySetIdsForDataSource
                                )
                                ps.setArray(1, idsArr)
                                ps.setArray(2, entitySetIdsArr)
                                ps.setArray(3, idsArr)
                                ps.setArray(4, entitySetIdsArr)
                            }) {
                        ResultSetAdapters.edge(it)
                    }.toList()
                }

        return edges.stream()
    }


    @Timed
    override fun getNeighborEntitySets(
            entitySetIds: Set<UUID>
    ): List<NeighborSets> {
        val neighbors: MutableList<NeighborSets> = ArrayList()

        val query = "SELECT DISTINCT ${SRC_ENTITY_SET_ID.name},${EDGE_ENTITY_SET_ID.name}, ${DST_ENTITY_SET_ID.name} " +
                "FROM ${E.name} " +
                "WHERE ( ${SRC_ENTITY_SET_ID.name} = ANY(?) OR ${DST_ENTITY_SET_ID.name} = ANY(?) ) " +
                "AND ${VERSION.name} > 0"

        val groupedEntitySets = entitySetIds.groupBy { dataSourceResolver.getDataSourceName(it) }
        groupedEntitySets.forEach { (dataSourceName, entitySetIdsForDataSource) ->
            val reader = dataSourceResolver.getDataSource(dataSourceName)
            val connection = reader.connection
            connection.use {
                val ps = connection.prepareStatement(query)
                ps.use {
                    val entitySetIdsArr = PostgresArrays.createUuidArray(
                            connection,
                            groupedEntitySets.getValue(dataSourceName)
                    )
                    ps.setArray(1, entitySetIdsArr)
                    ps.setArray(2, entitySetIdsArr)
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
        }
        return neighbors
    }

    override fun getEdgeEntitySetsConnectedToEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>
    ): Set<UUID> {
        val query = "SELECT DISTINCT ${EDGE_ENTITY_SET_ID.name} " +
                "FROM ${E.name} " +
                "WHERE ($SRC_IDS_SQL) OR ($DST_IDS_SQL) "
        val reader = dataSourceResolver.resolve(entitySetId)
        return BasePostgresIterable(PreparedStatementHolderSupplier(reader, query) { ps ->
            val entityKeyIdArr = PostgresArrays.createUuidArray(ps.connection, entityKeyIds)
            ps.setArray(1, entityKeyIdArr)
            ps.setObject(2, entitySetId)
            ps.setArray(3, entityKeyIdArr)
            ps.setObject(4, entitySetId)
        }) {
            ResultSetAdapters.edgeEntitySetId(it)
        }.toSet()
    }

    @Timed
    override fun checkForUnauthorizedEdges(
            entitySetId: UUID,
            authorizedEdgeEntitySets: Set<UUID>,
            entityKeyIds: Set<UUID>?
    ): Boolean {
        val notAssocClause = if (authorizedEdgeEntitySets.isEmpty()) "" else {
            "AND NOT( ${EDGE_ENTITY_SET_ID.name} = ANY('{${authorizedEdgeEntitySets.joinToString()}}') )"
        }

        val srcEntitySetFilter = entitySetClause(entitySetId, entityKeyIds, SRC_ENTITY_SET_ID, SRC_ENTITY_KEY_ID)
        val dstEntitySetFilter = entitySetClause(entitySetId, entityKeyIds, DST_ENTITY_SET_ID, DST_ENTITY_KEY_ID)

        val query = """
            SELECT 1 FROM ${E.name}
            WHERE
              ( ($srcEntitySetFilter) OR ($dstEntitySetFilter) )
              $notAssocClause
            LIMIT 1
        """.trimIndent()
        val hds = dataSourceResolver.resolve(entitySetId)
        hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(query).use { rs ->
                    return rs.next()
                }
            }
        }
    }

    private fun entitySetClause(
            entitySetId: UUID, entityKeyIds: Set<UUID>?, entitySetColumn: PostgresColumnDefinition,
            entityKeyIdColumn: PostgresColumnDefinition
    ): String {
        val entityKeyIdClause = if (entityKeyIds.isNullOrEmpty()) "" else {
            " AND ${entityKeyIdColumn.name} = ANY('{${entityKeyIds.joinToString()}}')"
        }

        return "${entitySetColumn.name} = '$entitySetId' $entityKeyIdClause"
    }

    private fun buildAssociationTable(
            index: Int,
            selfEntitySetIds: Set<UUID>,
            authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
            linked: Boolean
    ): String {
        val esEntityKeyIds = entitySetManager
                .getEntitySetIdsOfType(
                        authorizedFilteredRanking.filteredNeighborsRanking.associationTypeId
                )
                .map { it to Optional.empty<Set<UUID>>() }
                .toMap()
        val associationPropertyTypes = authorizedFilteredRanking.associationPropertyTypes
        //This will read all the entity sets of the same type all at once applying filters.
        val dataSql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                esEntityKeyIds,
                associationPropertyTypes.mapValues {
                    quote(
                            it.value.type.fullQualifiedNameAsString
                    )
                },
                authorizedFilteredRanking.filteredNeighborsRanking.associationAggregations.keys,
                authorizedFilteredRanking.associationSets,
                authorizedFilteredRanking.filteredNeighborsRanking.associationFilters,
                setOf(),
                associationPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                linking = false,
                omitEntitySetId = false
        )

        val joinColumns = entityKeyIdColumns
        val groupingColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"

        val spineSql = buildSpineSql(
                selfEntitySetIds, authorizedFilteredRanking, linked, true
        )

        val aggregationColumns =
                authorizedFilteredRanking.filteredNeighborsRanking.associationAggregations
                        .mapValues {
                            val fqn = quote(
                                    associationPropertyTypes.getValue(
                                            it.key
                                    ).type.fullQualifiedNameAsString
                            )
                            val alias = aggregationAssociationColumnName(index, it.key)
                            "${it.value.type.name}(COALESCE($fqn[1],0)) as $alias"
                        }.values.joinToString(",")
        val countAlias = associationCountColumnName(index)
        val allColumns = listOf(
                groupingColumns, aggregationColumns, "count(*) as $countAlias"
        )
                .filter(String::isNotBlank)
                .joinToString(",")
        val nullCheck = if (linked) "WHERE ${LINKING_ID.name} IS NOT NULL" else ""
        return "SELECT $allColumns " +
                "FROM ($spineSql $nullCheck) as spine " +
                "INNER JOIN ($dataSql) as data USING($joinColumns) " +
                "GROUP BY ($groupingColumns)"
    }


    private fun buildEntityTable(
            index: Int,
            selfEntitySetIds: Set<UUID>,
            authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
            linked: Boolean
    ): String {
        val esEntityKeyIds = entitySetManager
                .getEntitySetIdsOfType(
                        authorizedFilteredRanking.filteredNeighborsRanking.neighborTypeId
                )
                .map { it to Optional.empty<Set<UUID>>() }
                .toMap()
        val entitySetPropertyTypes = authorizedFilteredRanking.entitySetPropertyTypes
        //This will read all the entity sets of the same type all at once applying filters.
        val dataSql = selectEntitySetWithCurrentVersionOfPropertyTypes(
                esEntityKeyIds,
                entitySetPropertyTypes.mapValues {
                    quote(
                            it.value.type.fullQualifiedNameAsString
                    )
                },
                authorizedFilteredRanking.filteredNeighborsRanking.neighborTypeAggregations.keys,
                authorizedFilteredRanking.entitySets,
                authorizedFilteredRanking.filteredNeighborsRanking.neighborFilters,
                setOf(),
                entitySetPropertyTypes.mapValues { it.value.datatype == EdmPrimitiveTypeKind.Binary },
                linking = false,
                omitEntitySetId = false
        )


        val joinColumns = entityKeyIdColumns
        val groupingColumns = if (linked) LINKING_ID.name else "$SELF_ENTITY_SET_ID, $SELF_ENTITY_KEY_ID"

        val spineSql = buildSpineSql(
                selfEntitySetIds, authorizedFilteredRanking, linked, false
        )

        val aggregationColumns =
                authorizedFilteredRanking.filteredNeighborsRanking.neighborTypeAggregations
                        .mapValues {
                            val fqn = quote(
                                    entitySetPropertyTypes.getValue(
                                            it.key
                                    ).type.fullQualifiedNameAsString
                            )
                            val alias = aggregationEntityColumnName(index, it.key)
                            "${it.value.type.name}(COALESCE($fqn[1],0)) as $alias"
                        }.values.joinToString(",")
        val countAlias = entityCountColumnName(index)
        val allColumns = listOf(
                groupingColumns, aggregationColumns, "count(*) as $countAlias"
        )
                .filter(String::isNotBlank)
                .joinToString(",")
        val nullCheck = if (linked) "WHERE ${LINKING_ID.name} IS NOT NULL" else ""
        return "SELECT $allColumns " +
                "FROM ($spineSql $nullCheck) as spine " +
                "INNER JOIN ($dataSql) as data USING($joinColumns) " +
                "GROUP BY ($groupingColumns)"
    }
}

internal fun getLinkingId(linkingIds: Map<UUID, UUID>, entityKeyId: UUID): UUID = linkingIds.getValue(entityKeyId)

private val KEY_COLUMNS = E.primaryKey.map { col -> col.name }.toSet()

private val INSERT_COLUMNS = E.columns.filterNot { LAST_TRANSPORT == it }.map { it.name }.toSet()

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
@Deprecated("Edges table queries need update")
internal fun getTopUtilizersSql(
        entitySetId: UUID,
        srcFilters: SetMultimap<UUID, UUID>,
        dstFilters: SetMultimap<UUID, UUID>,
        top: Int = 100
): String {

    return "SELECT ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}, (COALESCE(src_count,0) + COALESCE(dst_count,0)) as total_count " +
            "FROM (${getTopUtilizersFromSrc(entitySetId, srcFilters)}) as src_counts " +
            "FULL OUTER JOIN (${getTopUtilizersFromDst(entitySetId, dstFilters)}) as dst_counts " +
            "USING(${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}) " +
//            "WHERE total_count IS NOT NULL " +
            "ORDER BY total_count DESC " +
            "LIMIT $top"

}

@Deprecated("Edges table queries need update")
internal fun getTopUtilizersFromSrc(entitySetId: UUID, filters: SetMultimap<UUID, UUID>): String {
    val countColumn = "src_count"
    return "SELECT ${SRC_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${SRC_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, count(*) as $countColumn " +
            "FROM ${E.name} WHERE ${srcClauses(entitySetId, filters)} " +
            "GROUP BY (${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}) "
}

@Deprecated("Edges table queries need update")
internal fun getTopUtilizersFromDst(entitySetId: UUID, filters: SetMultimap<UUID, UUID>): String {
    val countColumn = "dst_count"
    return "SELECT ${DST_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${DST_ENTITY_KEY_ID.name} as ${ID_VALUE.name}, count(*) as $countColumn " +
            "FROM ${E.name} WHERE ${dstClauses(entitySetId, filters)} " +
            "GROUP BY (${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name}) "
}


val EDGES_UPSERT_SQL = "INSERT INTO ${E.name} (${INSERT_COLUMNS.joinToString(",")}) " +
        "VALUES (${(INSERT_COLUMNS.indices).joinToString(",") { "?" }}) " +
        "ON CONFLICT (${KEY_COLUMNS.joinToString(",")}) " +
        "DO UPDATE SET ${VERSION.name} = EXCLUDED.${VERSION.name}, " +
        "${VERSIONS.name} = ${E.name}.${VERSIONS.name} || EXCLUDED.${VERSION.name}"


private val CLEAR_SQL = "UPDATE ${E.name} SET ${VERSION.name} = ?, ${VERSIONS.name} = ${VERSIONS.name} || ? WHERE "
private val DELETE_SQL = "DELETE FROM ${E.name} WHERE "
private val LOCK_SQL1 = "SELECT 1 FROM ${E.name} WHERE "
private const val LOCK_SQL2 = " FOR UPDATE"

private val VERTEX_FILTER_SQL = "${KEY_COLUMNS.joinToString(" = ? AND ")} = ? "

private val CLEAR_BY_VERTEX_SQL = "$CLEAR_SQL $VERTEX_FILTER_SQL"
private val DELETE_BY_VERTEX_SQL = "$DELETE_SQL $VERTEX_FILTER_SQL"
private val LOCK_BY_VERTEX_SQL = "$LOCK_SQL1 $VERTEX_FILTER_SQL $LOCK_SQL2"

private val NEIGHBORHOOD_OF_ENTITY_SET_SQL = "SELECT * FROM ${E.name} WHERE " +
        "( (${SRC_ENTITY_SET_ID.name} = ?) OR (${EDGE_ENTITY_SET_ID.name} = ?) OR (${DST_ENTITY_SET_ID.name} = ?) )"

private val SRC_IDS_SQL = "${SRC_ENTITY_KEY_ID.name} = ANY(?) AND ${SRC_ENTITY_SET_ID.name} = ?"
private val EDGE_IDS_SQL = "${EDGE_ENTITY_KEY_ID.name} = ANY(?) AND ${EDGE_ENTITY_SET_ID.name} = ?"
private val DST_IDS_SQL = "${DST_ENTITY_KEY_ID.name} = ANY(?) AND ${DST_ENTITY_SET_ID.name} = ?"

private val DEFAULT_NEIGHBORHOOD_SRC_FILTER = "${SRC_ENTITY_KEY_ID.name} = ANY(?) AND ${SRC_ENTITY_SET_ID.name} = ANY(?) AND ${PARTITION.name} = ANY(?)"
private val DEFAULT_NEIGHBORHOOD_DST_FILTER = "${DST_ENTITY_KEY_ID.name} = ANY(?) AND ${DST_ENTITY_SET_ID.name} = ANY(?)"

//private val REPARTITION_SQL = "INSERT INTO ${E.name} SELECT $REPARTITION_COLUMNS FROM ${E.name} INNER JOIN (select id as entity_set_id, partitions from entity_sets) as es using (entity_set_id) WHERE entity_set_id = ? AND "

/**
 * Loads edges where either the source, destination, or association matches a set of entityKeyIds from a specific entity set
 *
 * 1. entityKeyIds
 * 2. entitySetId
 * 3. entityKeyIds
 * 4. entitySetId
 * 5. partitions (for vertex entity set or sets)
 * 5. entityKeyIds
 * 6. entitySetId
 */
private val BULK_NEIGHBORHOOD_SQL = "SELECT * FROM ${E.name} WHERE (($SRC_IDS_SQL) OR ($DST_IDS_SQL) OR ($EDGE_IDS_SQL))"
private val BULK_NON_TOMBSTONED_NEIGHBORHOOD_SQL = "$BULK_NEIGHBORHOOD_SQL AND ${VERSION.name} > 0"


private val PAGED_NEIGHBOR_SEARCH_ORDER_COLS = listOf(
        SRC_ENTITY_SET_ID,
        SRC_ENTITY_KEY_ID,
        EDGE_ENTITY_SET_ID,
        EDGE_ENTITY_KEY_ID,
        DST_ENTITY_SET_ID,
        DST_ENTITY_KEY_ID
).map { it.name }

/**
 * PreparedStatement bind order (note: these bind params all apply to the vertex entity set):
 *
 * 1. entityKeyIds
 * 2. entitySetIds
 * 3. entityKeyIds
 * 4. entitySetIds
 * 5. partitions
 */
internal fun getFilteredNeighborhoodSql(
        pagedNeighborRequest: PagedNeighborRequest,
): String {

    val filter = pagedNeighborRequest.filter
    val limit = pagedNeighborRequest.pageSize
    val bookmark = pagedNeighborRequest.bookmark


    var vertexAsSrcSql = DEFAULT_NEIGHBORHOOD_SRC_FILTER
    var vertexAsDstSql = DEFAULT_NEIGHBORHOOD_DST_FILTER

    if (filter.dstEntitySetIds.isPresent) {
        val dstEntitySetIdsSql = entitySetFilterClause(DST_ENTITY_SET_ID, filter.dstEntitySetIds)

        vertexAsSrcSql += " AND ( $dstEntitySetIdsSql )"
    }

    if (filter.srcEntitySetIds.isPresent) {
        val srcEntitySetIdsSql = entitySetFilterClause(SRC_ENTITY_SET_ID, filter.srcEntitySetIds)

        vertexAsDstSql += " AND ( $srcEntitySetIdsSql )"
    }

    if (filter.associationEntitySetIds.isPresent && filter.associationEntitySetIds.get().isNotEmpty()) {
        val associationEntitySetIdsSql = entitySetFilterClause(EDGE_ENTITY_SET_ID, filter.associationEntitySetIds)

        vertexAsSrcSql += " AND ( $associationEntitySetIdsSql )"
        vertexAsDstSql += " AND ( $associationEntitySetIdsSql )"
    }

    val bookmarkClause = if (bookmark != null) {
        """
            AND ( ${PAGED_NEIGHBOR_SEARCH_ORDER_COLS.joinToString()} ) > (
              '${bookmark.src.entitySetId}',
              '${bookmark.src.entityKeyId}',
              '${bookmark.edge.entitySetId}',
              '${bookmark.edge.entityKeyId}',
              '${bookmark.dst.entitySetId}',
              '${bookmark.dst.entityKeyId}'
            )
        """.trimIndent()
    } else {
        ""
    }

    val pagingClause = if (limit > 0) "ORDER BY ${PAGED_NEIGHBOR_SEARCH_ORDER_COLS.joinToString()} LIMIT $limit" else ""


    return """
      SELECT *
      FROM ${E.name}
      WHERE 
        ( ( $vertexAsDstSql ) OR ( $vertexAsSrcSql ) ) 
        AND ${VERSION.name} > 0
        $bookmarkClause
      $pagingClause
   """.trimIndent()
}

private fun entitySetFilterClause(column: PostgresColumnDefinition, entitySetFilter: Optional<Set<UUID>>): String {
    return if (entitySetFilter.get().isNotEmpty()) {
        "${column.name} IN (${
            entitySetFilter.get().joinToString { "'$it'" }
        })"
    } else {
        "false"
    }
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
        authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
        association: Boolean,
        isDst: Boolean
): String {
    val authorizedAssociationEntitySets = authorizedFilteredRanking.associationSets.keys
    val authorizedEntitySets = authorizedFilteredRanking.entitySets.keys

    val associationsClause =
            "${EDGE_ENTITY_SET_ID.name} IN (${authorizedAssociationEntitySets.joinToString(",") { "'$it'" }})"

    val entitySetColumn = if (isDst) {
        DST_ENTITY_SET_ID.name
    } else {
        SRC_ENTITY_SET_ID.name
    }

    val selfEntitySetColumn = if (isDst) {
        SRC_ENTITY_SET_ID.name
    } else {
        DST_ENTITY_SET_ID.name
    }

    val entitySetsClause =
            "$entitySetColumn IN (${authorizedEntitySets.joinToString(",") { "'$it'" }}) " +
                    "AND $selfEntitySetColumn IN (${selfEntitySetIds.joinToString(",") { "'$it'" }})"

    return "($associationsClause AND $entitySetsClause)"
}

private fun buildSpineSql(
        selfEntitySetIds: Set<UUID>,
        authorizedFilteredRanking: AuthorizedFilteredNeighborsRanking,
        linked: Boolean,
        association: Boolean
): String {
    val isDst = authorizedFilteredRanking.filteredNeighborsRanking.dst


    val baseEntityColumnsSql = if (association) {
        // Order on which we select is {edge, src, dst}, EDGE IdType
        if (isDst) { // select src and edge
            "${SRC_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_1.name} as $SELF_ENTITY_KEY_ID, " +
                    "${EDGE_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        } else { // select dst and edge
            "${DST_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_2.name} as $SELF_ENTITY_KEY_ID, " +
                    "${EDGE_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        }
    } else {
        // Order on which we select is {src, dst, edge}, DST IdType
        if (isDst) { // select src and dst
            "${SRC_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_2.name} as $SELF_ENTITY_KEY_ID, " +
                    "${DST_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        } else { // Order on which we select is {src, dst, edge}, SRC IdType
            // select dst and src
            "${DST_ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${EDGE_COMP_1.name} as $SELF_ENTITY_KEY_ID, " +
                    "${SRC_ENTITY_SET_ID.name} as ${PostgresColumn.ENTITY_SET_ID.name}, ${ID_VALUE.name} as ${ID_VALUE.name}"
        }
    }

    val edgeClause = buildEdgeFilteringClause(selfEntitySetIds, authorizedFilteredRanking, association, isDst)
    val idSql = "SELECT ${PostgresColumn.ENTITY_SET_ID.name} as $SELF_ENTITY_SET_ID, ${PostgresColumn.ID.name} as $SELF_ENTITY_KEY_ID, ${LINKING_ID.name} FROM ${IDS.name}"


    return if (linked) {
        "SELECT edges.*, ${LINKING_ID.name} FROM (SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause) as edges " +
                "LEFT JOIN ($idSql) as ${IDS.name} USING ($SELF_ENTITY_SET_ID,$SELF_ENTITY_KEY_ID)"
    } else {
        "SELECT DISTINCT $baseEntityColumnsSql FROM edges WHERE $edgeClause"
    }
}

/**
 * Generates an association clause for querying the edges table.
 * @param entitySetColumn The column to use for filtering allowed entity set ids.
 * @param entitySetId The entity set id for which the aggregation will be performed.
 * @param neighborColumn The column for the neighbors that will be counted.
 * @param associationFilters A multimap from association entity set ids to allowed neighbor entity set ids.
 */
private fun associationClauses(
        entitySetColumn: String, entitySetId: UUID, neighborColumn: String,
        associationFilters: SetMultimap<UUID, UUID>
): String {
    if (associationFilters.isEmpty) {
        return " false "
    }
    return Multimaps
            .asMap(associationFilters)
            .asSequence()
            .map { (key, value) ->
                "($entitySetColumn = '$entitySetId' AND ${EDGE_ENTITY_SET_ID.name} = '$key' " +
                        "AND $neighborColumn IN (${value.joinToString(",") { "'$it'" }}) ) "
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

fun bindColumnsForEdge(
        ps: PreparedStatement,
        dataEdgeKey: DataEdgeKey,
        version: Long,
        versions: java.sql.Array,
) {
    var index = 1

    ps.setObject(index++, dataEdgeKey.src.entitySetId)
    ps.setObject(index++, dataEdgeKey.src.entityKeyId)
    ps.setObject(index++, dataEdgeKey.dst.entitySetId)
    ps.setObject(index++, dataEdgeKey.dst.entityKeyId)
    ps.setObject(index++, dataEdgeKey.edge.entitySetId)
    ps.setObject(index++, dataEdgeKey.edge.entityKeyId)
    ps.setLong(index++, version)
    ps.setArray(index++, versions)
    ps.addBatch()
}

/**
 * PreparedStatement bind order:
 *
 * 1) version
 * 2) version
 * 1) entitySetId
 * 2) entityKeyId
 */
@JsonIgnore
private val SOFT_DELETE_EDGES_SQL = """
            UPDATE ${E.name}
            SET
              ${VERSION.name} = ?,
              ${VERSIONS.name} = ${VERSIONS.name} || ?
            WHERE
              ${PARTITION.name} = ? AND
              ${SRC_ENTITY_KEY_ID.name} = ? AND
              ${DST_ENTITY_KEY_ID.name} = ? AND
              ${EDGE_ENTITY_KEY_ID.name} = ? 
        """.trimIndent()

/**
 * PreparedStatement bind order:
 *
 * 1) entitySetId
 * 2) entityKeyId
 */
@JsonIgnore
private val HARD_DELETE_EDGES_SQL = """
            DELETE FROM ${E.name}
            WHERE
              ${PARTITION.name} = ? AND
              ${SRC_ENTITY_KEY_ID.name} = ? AND
              ${DST_ENTITY_KEY_ID.name} = ? AND
              ${EDGE_ENTITY_KEY_ID.name} = ? 
        """.trimIndent()
