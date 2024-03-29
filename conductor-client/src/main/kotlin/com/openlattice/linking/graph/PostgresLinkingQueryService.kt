/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.linking.graph

import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.postgres.createOrUpdateLinkFromEntity
import com.openlattice.data.storage.postgres.tombstoneLinkForEntity
import com.openlattice.data.storage.postgres.updateLinkingId
import com.openlattice.linking.EntityKeyPair
import com.openlattice.linking.LinkingQueryService
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.DST_ENTITY_KEY_ID
import com.openlattice.postgres.PostgresColumn.DST_ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ENTITY_KEY_IDS_COL
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ENTITY_TYPE_ID
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.ID_VALUE
import com.openlattice.postgres.PostgresColumn.LINKING_ID
import com.openlattice.postgres.PostgresColumn.SCORE
import com.openlattice.postgres.PostgresColumn.SRC_ENTITY_KEY_ID
import com.openlattice.postgres.PostgresColumn.SRC_ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.PostgresTable.MATCHED_ENTITIES
import com.openlattice.postgres.ResultSetAdapters
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolder
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*


/**
 * The class implements the necessary SQL queries and logic for linking operations as defined by [LinkingQueryService].
 *
 * @param hds A hikari datasource that can be used for executing SQL.
 */
class PostgresLinkingQueryService(
        private val hds: HikariDataSource
) : LinkingQueryService {

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresLinkingQueryService::class.java)

        fun deleteNeighborhoods(hds: HikariDataSource, entitySetId: UUID, entityKeyIds: Set<UUID>): Int {
            hds.connection.use { connection ->
                val arr = PostgresArrays.createUuidArray(connection, entityKeyIds)
                connection.prepareStatement(DELETE_NEIGHBORHOODS_SQL).use { ps ->
                    ps.setObject(1, entitySetId)
                    ps.setArray(2, arr)
                    ps.setObject(3, entitySetId)
                    ps.setArray(4, arr)
                    return ps.executeUpdate()
                }
            }
        }
    }

    override fun lockClustersForUpdates(clusters: Set<UUID>): Connection {
        val connection = hds.connection
        connection.autoCommit = false

        val psLocks = connection.prepareStatement(LOCK_CLUSTERS_SQL)
        clusters.toSortedSet().forEach {
            psLocks.setObject(1, it)
            psLocks.addBatch()
        }
        psLocks.executeBatch()

        return connection
    }

    override fun lockClustersDoWorkAndCommit(
            candidate: EntityDataKey,
            candidates: Set<EntityDataKey>,
            doWork: (clusters: Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>>) -> Triple<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>, Boolean>
    ): Triple<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>, Boolean> {
        val clusters = getClustersForIds(candidates)

        lockClustersForUpdates(clusters.keys).use { conn ->
            try {
                val resultTriple = doWork(clusters)
                val linkingId = resultTriple.first
                val scores = resultTriple.second
                insertMatchScores(conn, linkingId, scores)
                return resultTriple
            } catch (ex: Exception) {
                conn.rollback()
                throw ex
            }
        }
    }

    override fun getLinkableEntitySets(
            linkableEntityTypeIds: Set<UUID>,
            entitySetBlacklist: Set<UUID>,
            whitelist: Set<UUID>
    ): BasePostgresIterable<UUID> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, LINKABLE_ENTITY_SET_IDS) { ps ->
            val entityTypeArr = PostgresArrays.createUuidArray(ps.connection, linkableEntityTypeIds)
            val blackListArr = PostgresArrays.createUuidArray(ps.connection, entitySetBlacklist)
            val whitelistArr = PostgresArrays.createUuidArray(ps.connection, whitelist)
            ps.setArray(1, entityTypeArr)
            ps.setArray(2, blackListArr)
            ps.setArray(3, whitelistArr)
        }) { ResultSetAdapters.id(it) }
    }

    override fun getEntitiesNeedingLinking(entitySetId: UUID, limit: Int): BasePostgresIterable<EntityDataKey> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, ENTITY_KEY_IDS_NEEDING_LINKING) { ps ->
            ps.setObject(1, entitySetId)
            ps.setInt(2, limit)
        }) { EntityDataKey(ResultSetAdapters.entitySetId(it), ResultSetAdapters.id(it)) }
    }

    override fun getEntitiesNotLinked(entitySetIds: Set<UUID>, limit: Int): BasePostgresIterable<Pair<UUID, UUID>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, ENTITY_KEY_IDS_NOT_LINKED) { ps ->
            val arr = PostgresArrays.createUuidArray(ps.connection, entitySetIds)
            ps.setArray(1, arr)
            ps.setInt(2, limit)
            val rs = ps.executeQuery()
            StatementHolder(ps.connection, ps, rs)
        }) { ResultSetAdapters.entitySetId(it) to ResultSetAdapters.id(it) }
    }

    // Unused
    override fun updateIdsTable(clusterId: UUID, newMember: EntityDataKey): Int {
        //Does not need locking only affects a single row.
        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(UPDATE_LINKED_ENTITIES_SQL)
            ps.setObject(1, clusterId)
            ps.setObject(2, newMember.entitySetId)
            ps.setObject(3, newMember.entityKeyId)
            ps.executeUpdate()
        }
    }

    override fun getClustersForIds(
            dataKeys: Set<EntityDataKey>
    ): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>> {
        if (dataKeys.isEmpty()) {
            return mapOf()
        }

        return BasePostgresIterable(StatementHolderSupplier(hds, buildClusterContainingSql(dataKeys))) {
            val linkingId = ResultSetAdapters.linkingId(it)
            val src = ResultSetAdapters.srcEntityDataKey(it)
            val dst = ResultSetAdapters.dstEntityDataKey(it)
            val score = ResultSetAdapters.score(it)
            linkingId to (src to (dst to score))
        }
                .groupBy({ it.first }, { it.second })
                .mapValues {
                    it.value.groupBy({ src -> src.first }, { dstScore -> dstScore.second })
                            .mapValues { dstScores -> dstScores.value.toMap() }
                }
    }

    override fun createOrUpdateLink(linkingId: UUID, cluster: Map<UUID, LinkedHashSet<UUID>>) {
        hds.connection.use { connection ->
            connection.prepareStatement(createOrUpdateLinkFromEntity()).use { ps ->
                val version = System.currentTimeMillis()
                cluster.forEach { (esid, ekids) ->
                    ekids.forEach { ekid ->
                        ps.setObject(1, linkingId)
                        ps.setLong(2, version)
                        ps.setObject(3, esid)
                        ps.setObject(4, ekid)
                        ps.addBatch()
                    }
                }
                ps.executeUpdate()
            }
        }
    }

    override fun createLinks(linkingId: UUID, toAdd: Set<EntityDataKey>): Int {
        hds.connection.use { connection ->
            connection.prepareStatement(createOrUpdateLinkFromEntity()).use { ps ->
                val version = System.currentTimeMillis()

                toAdd.forEach { edk ->
                    ps.setObject(1, linkingId) // ID value
                    ps.setLong(2, version)
                    ps.setObject(3, edk.entitySetId) // esid
                    ps.setObject(4, edk.entityKeyId) // origin id
                    ps.addBatch()
                }
                return ps.executeUpdate()
            }
        }
    }

    override fun updateLinkingInformation(
            linkingId: UUID,
            newMember: EntityDataKey,
            cluster: Map<UUID, LinkedHashSet<UUID>>
    ) {
        hds.connection.use { connection ->
            connection.prepareStatement(updateLinkingId()).use { dataPs ->
                cluster.forEach { (esid, ekids) ->
                    ekids.forEach { ekid ->
                        dataPs.setObject(1, linkingId)
                        dataPs.setObject(2, esid)
                        dataPs.setObject(3, ekid)
                        dataPs.addBatch()
                    }
                    dataPs.executeBatch()
                }
            }
            connection.prepareStatement(UPDATE_LINKED_ENTITIES_SQL).use { idsPs ->
                idsPs.setObject(1, linkingId)
                idsPs.setObject(2, newMember.entitySetId)
                idsPs.setObject(3, newMember.entityKeyId)
                idsPs.executeUpdate()
            }
        }
    }

    override fun tombstoneLinks(linkingId: UUID, toRemove: Set<EntityDataKey>): Int {
        hds.connection.use { connection ->
            connection.prepareStatement(tombstoneLinkForEntity).use { ps ->
                val version = System.currentTimeMillis()
                toRemove.forEach { edk ->
                    ps.setLong(1, version)
                    ps.setLong(2, version)
                    ps.setLong(3, version)
                    ps.setObject(4, edk.entitySetId) // esid
                    ps.setObject(5, linkingId) // ID value
                    ps.setObject(6, edk.entityKeyId) // origin id
                    ps.addBatch()
                }
                return ps.executeUpdate()
            }
        }
    }

    override fun getClusterFromLinkingId(linkingId: UUID): Map<EntityDataKey, Map<EntityDataKey, Double>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, CLUSTER_CONTAINING_SQL) { ps ->
            ps.setObject(1, linkingId)
        }) {
            val src = ResultSetAdapters.srcEntityDataKey(it)
            val dst = ResultSetAdapters.dstEntityDataKey(it)
            val score = ResultSetAdapters.score(it)
            src to (dst to score)
        }
                .groupBy({ it.first }, { it.second })
                .mapValues {
                    it.value.toMap()
                }
    }

    override fun insertMatchScores(
            connection: Connection,
            clusterId: UUID,
            scores: Map<EntityDataKey, Map<EntityDataKey, Double>>
    ): Int {
        connection.use { conn ->
            conn.prepareStatement(INSERT_SQL).use { ps ->
                scores.forEach { (srcEntityDataKey, dst) ->
                    dst.forEach { (dstEntityDataKey, score) ->
                        ps.setObject(1, clusterId)
                        ps.setObject(2, srcEntityDataKey.entitySetId)
                        ps.setObject(3, srcEntityDataKey.entityKeyId)
                        ps.setObject(4, dstEntityDataKey.entitySetId)
                        ps.setObject(5, dstEntityDataKey.entityKeyId)
                        ps.setDouble(6, score)
                        ps.addBatch()
                    }
                }
                val insertCount = ps.executeBatch().sum()
                conn.commit()
                return insertCount
            }
        }
    }

    override fun deleteNeighborhood(entity: EntityDataKey, positiveFeedbacks: Collection<EntityKeyPair>): Int {
        val deleteNeighborHoodSql = DELETE_NEIGHBORHOOD_SQL +
                if (positiveFeedbacks.isNotEmpty()) " AND NOT ( ${
                    buildFilterEntityKeyPairs(
                            positiveFeedbacks
                    )
                } )" else ""
        hds.connection.use { conn ->
            conn.prepareStatement(deleteNeighborHoodSql).use { ps ->
                ps.setObject(1, entity.entitySetId)
                ps.setObject(2, entity.entityKeyId)
                ps.setObject(3, entity.entitySetId)
                ps.setObject(4, entity.entityKeyId)
                return ps.executeUpdate()
            }
        }
    }

    override fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, ENTITY_KEY_IDS_OF_LINKING_IDS_SQL) { ps ->

            val linkingIdsArray = PostgresArrays.createUuidArray(ps.connection, linkingIds)

            ps.setArray(1, linkingIdsArray)
            ps.setArray(2, PostgresArrays.createUuidArray(ps.connection, normalEntitySetIds))
        }) { rs ->
            val linkingId = ResultSetAdapters.linkingId(rs)
            val entityKeyIds = ResultSetAdapters.entityKeyIds(rs)
            linkingId to entityKeyIds
        }
    }

}

internal fun uuidString(id: UUID): String {
    return "'$id'::uuid"
}

/**
 * MATCHED_ENTITIES Queries
 */
private val COLUMNS = MATCHED_ENTITIES.columns.joinToString(",") { it.name }

internal fun buildClusterContainingSql(dataKeys: Set<EntityDataKey>): String {
    val dataKeysSql = dataKeys.joinToString(",") { "('${it.entitySetId}','${it.entityKeyId}')" }
    return "SELECT * " +
            "FROM ${MATCHED_ENTITIES.name} " +
            "WHERE ((${SRC_ENTITY_SET_ID.name},${SRC_ENTITY_KEY_ID.name}) IN ($dataKeysSql)) " +
            "OR ((${DST_ENTITY_SET_ID.name},${DST_ENTITY_KEY_ID.name}) IN ($dataKeysSql))"
}

internal fun buildFilterEntityKeyPairs(entityKeyPairs: Collection<EntityKeyPair>): String {
    return entityKeyPairs.joinToString(" OR ") {
        val firstEsidString = uuidString(it.first.entitySetId)
        val firstEkidString = uuidString(it.first.entityKeyId)
        val secondEsidString = uuidString(it.second.entitySetId)
        val secondEkidString = uuidString(it.second.entityKeyId)
        """
        ( 
            (${SRC_ENTITY_SET_ID.name} = $firstEsidString 
                AND ${SRC_ENTITY_KEY_ID.name} = $firstEkidString
                AND ${DST_ENTITY_SET_ID.name} = $secondEsidString
                AND ${DST_ENTITY_KEY_ID.name} = $secondEkidString
            ) OR (  ${SRC_ENTITY_SET_ID.name} = $secondEsidString
                AND ${SRC_ENTITY_KEY_ID.name} = $secondEkidString
                AND ${DST_ENTITY_SET_ID.name} = $firstEsidString
                AND ${DST_ENTITY_KEY_ID.name} = $firstEkidString
            ) 
        )
        """.trimIndent()
    }
}

// @formatter:off
/**
 * SQL to select normal entity key ids of linking ids. Bind order is as folows:
 *
 * 1. linkingIds
 * 2. normalEntitySetIds
 */
private val ENTITY_KEY_IDS_OF_LINKING_IDS_SQL =
        "SELECT ${LINKING_ID.name}, array_agg(${ID.name}) AS ${ENTITY_KEY_IDS_COL.name} " +
        "FROM ${IDS.name} " +
        "WHERE ${VERSION.name} > 0 " +
            "AND ${LINKING_ID.name} IS NOT NULL " +
            "AND ${LINKING_ID.name} = ANY( ? ) " +
            "AND ${ENTITY_SET_ID.name} = ANY(?) " +
        "GROUP BY ${LINKING_ID.name}"

private val LOCK_CLUSTERS_SQL = "SELECT 1 FROM ${MATCHED_ENTITIES.name} WHERE ${LINKING_ID.name} = ? FOR UPDATE"

private val CLUSTER_CONTAINING_SQL = "SELECT * FROM ${MATCHED_ENTITIES.name} WHERE ${LINKING_ID.name} = ANY(?)"

private val DELETE_NEIGHBORHOOD_SQL = "DELETE FROM ${MATCHED_ENTITIES.name} " +
        "WHERE ( (${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ?) " +
        "OR (${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ?) )"

private val DELETE_NEIGHBORHOODS_SQL = "DELETE FROM ${MATCHED_ENTITIES.name} WHERE " +
        "(${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ANY(?)) OR " +
        "(${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ANY(?)) "

private val DELETE_ENTITY_SET_NEIGHBORHOOD_SQL = "DELETE FROM ${MATCHED_ENTITIES.name} " +
        "WHERE ${SRC_ENTITY_SET_ID.name} = ? OR ${DST_ENTITY_SET_ID.name} = ? "

private val INSERT_SQL = "INSERT INTO ${MATCHED_ENTITIES.name} ($COLUMNS) VALUES (?,?,?,?,?,?) " +
        "ON CONFLICT ON CONSTRAINT matched_entities_pkey " +
        "DO UPDATE SET ${SCORE.name} = EXCLUDED.${SCORE.name}"

/**
 * IDS queries
 */
private val UPDATE_LINKED_ENTITIES_SQL = """
        UPDATE ${IDS.name} 
        SET ${LINKING_ID.name} = ?, ${LAST_LINK.name} = now() 
        WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ? 
""".trimIndent()

private val ENTITY_KEY_IDS_NEEDING_LINKING = """
        SELECT ${ENTITY_SET_ID.name},${ID.name}
        FROM ${IDS.name}
        WHERE ${ENTITY_SET_ID.name} = ?
            AND ${LAST_LINK.name} < ${LAST_WRITE.name}
            AND ( ${LAST_INDEX.name} >= ${LAST_WRITE.name} )
            AND ( ${LAST_INDEX.name} > '-infinity'::timestamptz )
            AND ${VERSION.name} > 0
        ORDER BY ${VERSION.name} ASC
        LIMIT ?
        """.trimIndent()

private val ENTITY_KEY_IDS_NOT_LINKED = "SELECT ${ENTITY_SET_ID.name},${ID.name} " +
        "FROM ${IDS.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ANY(?) " +
            "AND ${LAST_LINK.name} < ${LAST_WRITE.name} " +
            "AND ${VERSION.name} > 0 LIMIT ?"

private val LINKABLE_ENTITY_SET_IDS = "SELECT ${ID.name} " +
        "FROM ${ENTITY_SETS.name} " +
        "WHERE ${ENTITY_TYPE_ID.name} = ANY(?) AND NOT ${ID.name} = ANY(?) AND ${ID.name} = ANY(?) "
// @formatter:on
