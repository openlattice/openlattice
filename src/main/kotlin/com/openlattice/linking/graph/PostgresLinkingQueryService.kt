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

package com.openlattice.linking.graph

import com.openlattice.data.EntityDataKey
import com.openlattice.linking.LinkingQueryService
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

private const val BLOCK_SIZE_FIELD = "block_size"
private const val AVG_SCORE_FIELD = "avg_score"

/**
 * The class implements the necessary SQL queries and logic for linking operations as defined by [LinkingQueryService].
 *
 * @param hds A hikari datasource that can be used for executing SQL.
 */
class PostgresLinkingQueryService(private val hds: HikariDataSource) : LinkingQueryService {

    override fun getLinkableEntitySets(
            linkableEntityTypeIds: Set<UUID>,
            entitySetBlacklist: Set<UUID>,
            whitelist: Set<UUID>): PostgresIterable<UUID> {
        return PostgresIterable(Supplier {
            val connection = hds.connection
            val ps = connection.prepareStatement(LINKABLE_ENTITY_SET_IDS)
            val entityTypeArr = PostgresArrays.createUuidArray(connection, linkableEntityTypeIds)
            val blackListArr = PostgresArrays.createUuidArray(connection, entitySetBlacklist)
            val whitelistArr = PostgresArrays.createUuidArray(connection, whitelist)
            ps.setObject(1, entityTypeArr)
            ps.setObject(2, blackListArr)
            ps.setObject(3, whitelistArr)
            val rs = ps.executeQuery()
            StatementHolder(connection, ps, rs)
        }, Function { ResultSetAdapters.id(it) })
    }

    override fun getEntitiesNeedingLinking(entitySetIds: Set<UUID>, limit: Int): PostgresIterable<Pair<UUID, UUID>> {
        return PostgresIterable(Supplier {
            val connection = hds.connection
            val ps = connection.prepareStatement(ENTITY_KEY_IDS_NEEDING_LINKING)
            val arr = PostgresArrays.createUuidArray(connection, entitySetIds)
            ps.setObject(1, arr)
            ps.setObject(2, limit)
            val rs = ps.executeQuery()
            StatementHolder(connection, ps, rs)
        }, Function { ResultSetAdapters.entitySetId(it) to ResultSetAdapters.id(it) })
    }

    override fun getEntitiesNotLinked(entitySetIds: Set<UUID>, limit: Int): PostgresIterable<Pair<UUID, UUID>> {
        return PostgresIterable(Supplier {
            val connection = hds.connection
            val ps = connection.prepareStatement(ENTITY_KEY_IDS_NOT_LINKED)
            val arr = PostgresArrays.createUuidArray(connection, entitySetIds)
            ps.setObject(1, arr)
            ps.setObject(2, limit)
            val rs = ps.executeQuery()
            StatementHolder(connection, ps, rs)
        }, Function { ResultSetAdapters.entitySetId(it) to ResultSetAdapters.id(it) })
    }

    override fun updateLinkingTable(clusterId: UUID, newMember: EntityDataKey): Int {
        hds.connection.use {
            it.prepareStatement(UPDATE_LINKED_ENTITIES_SQL).use {
                it.setObject(1, clusterId)
                it.setObject(2, newMember.entitySetId)
                it.setObject(3, newMember.entityKeyId)
                return it.executeUpdate()
            }
        }
    }

    override fun getIdsOfClustersContaining(dataKeys: Set<EntityDataKey>): PostgresIterable<UUID> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(buildIdsOfClusterContainingSql(dataKeys))
                    StatementHolder(connection, stmt, rs)
                },
                Function {
                    ResultSetAdapters.clusterId(it)
                })

    }

    //TODO: Possible optimization is to avoid copying of array when invoking this function
    override fun getClustersContaining(
            clusterIds: Collection<UUID>
    ): Map<UUID, Map<EntityDataKey, Map<EntityDataKey, Double>>> {
        return PostgresIterable<Pair<UUID, Pair<EntityDataKey, Pair<EntityDataKey, Double>>>>(
                Supplier {
                    val connection = hds.connection
                    val ps = connection.prepareStatement(CLUSTER_CONTAINING_SQL)
                    val arr = PostgresArrays.createUuidArray(connection, clusterIds)
                    ps.setObject(1, arr)
                    val rs = ps.executeQuery()
                    StatementHolder(connection, ps, rs)
                },
                Function {
                    val clusterId = ResultSetAdapters.clusterId(it)
                    val src = ResultSetAdapters.srcEntityDataKey(it)
                    val dst = ResultSetAdapters.dstEntityDataKey(it)
                    val score = ResultSetAdapters.score(it)
                    clusterId to (src to (dst to score))
                })
                .groupBy({ it.first }, { it.second })
                .mapValues { it.value.groupBy({ it.first }, { it.second }).mapValues { it.value.toMap() } }
    }

    override fun getClustersBySize(): PostgresIterable<Pair<EntityDataKey, Double>> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(BLOCKS_BY_AVG_SCORE_SQL)
                    StatementHolder(connection, stmt, rs)
                },
                Function {
                    val edk = ResultSetAdapters.entityDataKey(it)
                    val avgMatch = it.getDouble(AVG_SCORE_FIELD)
                    edk to avgMatch
                }
        )
    }

    override fun getOrderedBlocks(): PostgresIterable<Pair<EntityDataKey, Long>> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.createStatement()
                    val rs = stmt.executeQuery(BLOCKS_BY_SIZE_SQL)
                    StatementHolder(connection, stmt, rs)
                },
                Function {
                    val edk = ResultSetAdapters.entityDataKey(it)
                    val blockSize = it.getLong(BLOCK_SIZE_FIELD)
                    edk to blockSize
                }
        )
    }

    override fun getNeighborhoodScores(u: EntityDataKey): Map<EntityDataKey, Double> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val ps = connection.prepareStatement(NEIGHBORHOOD_SQL)

                    ps.setObject(1, u.entitySetId)
                    ps.setObject(2, u.entityKeyId)
                    ps.setObject(3, u.entitySetId)
                    ps.setObject(4, u.entityKeyId)
                    val rs = ps.executeQuery()
                    StatementHolder(connection, ps, rs)
                },
                Function<ResultSet, Pair<EntityDataKey, Double>> {
                    /*
                     * We are storing it as computed from blocks. In the future we may want to include values where
                     * entity data key is in destination as well, but for now we keep it simple. If changing this
                     * make sure to update the NEIGHBORHOOD_SQL statement as well.
                     *
                     */

                    val dstEntityDataKey = ResultSetAdapters.dstEntityDataKey(it)
                    val score = ResultSetAdapters.score(it)
                    dstEntityDataKey to score
                }).toMap()
    }

    override fun insertMatchScores(clusterId: UUID, score: Map<EntityDataKey, Map<EntityDataKey, Double>>): Int {
        hds.connection.use {
            it.prepareStatement(INSERT_SQL).use {
                val ps = it
                score.forEach {
                    val srcEntityDataKey = it.key
                    it.value.forEach {
                        val dstEntityDataKey = it.key
                        val score = it.value
                        ps.setObject(1, clusterId)
                        ps.setObject(2, srcEntityDataKey.entitySetId)
                        ps.setObject(3, srcEntityDataKey.entityKeyId)
                        ps.setObject(4, dstEntityDataKey.entitySetId)
                        ps.setObject(5, dstEntityDataKey.entityKeyId)
                        ps.setObject(6, score)
                        ps.addBatch()
                    }
                }
                return ps.executeBatch().sum()
            }
        }
    }

    override fun insertMatchScore(
            clusterId: UUID, blockKey: EntityDataKey, blockElement: EntityDataKey, score: Double
    ): Int {
        hds.connection.use {
            it.prepareStatement(INSERT_SQL).use {
                it.setObject(1, clusterId)
                it.setObject(2, blockKey.entitySetId)
                it.setObject(3, blockKey.entityKeyId)
                it.setObject(4, blockElement.entitySetId)
                it.setObject(5, blockElement.entityKeyId)
                it.setObject(6, score)
                return it.executeUpdate()
            }
        }
    }

    override fun deleteMatchScore(u: EntityDataKey, v: EntityDataKey): Int {
        hds.connection.use {
            it.prepareStatement(DELETE_SQL).use {
                it.setObject(1, u.entitySetId)
                it.setObject(2, u.entityKeyId)
                it.setObject(3, v.entitySetId)
                it.setObject(4, v.entityKeyId)
                return it.executeUpdate()
            }
        }
    }

    override fun deleteNeighborhood(u: EntityDataKey): Int {
        hds.connection.use {
            it.prepareStatement(DELETE_NEIGHBORHOOD_SQL).use {
                it.setObject(1, u.entitySetId)
                it.setObject(2, u.entityKeyId)
                it.setObject(3, u.entitySetId)
                it.setObject(4, u.entityKeyId)
                return it.executeUpdate()
            }
        }
    }

    override fun deleteNeighborhoods(entitySetId: UUID, entityKeyIds: Set<UUID>): Int {
        hds.connection.use {
            val arr = PostgresArrays.createUuidArray(it, entityKeyIds)
            it.prepareStatement(DELETE_NEIGHBORHOODS_SQL).use {
                it.setObject(1, entitySetId)
                it.setArray(2, arr)
                it.setObject(3, entitySetId)
                it.setArray(4, arr)
                return it.executeUpdate()
            }
        }
    }
}

internal fun buildIdsOfClusterContainingSql(dataKeys: Set<EntityDataKey>): String {
    val dataKeysSql = dataKeys.joinToString(",") { "('${it.entitySetId}','${it.entityKeyId}')" }
    return "SELECT distinct linking_id FROM ${MATCHED_ENTITIES.name} " +
            "WHERE ((${SRC_ENTITY_SET_ID.name},${SRC_ENTITY_KEY_ID.name}) IN ($dataKeysSql)) " +
            "OR ((${DST_ENTITY_SET_ID.name},${DST_ENTITY_KEY_ID.name}) IN ($dataKeysSql))"
}


private val COLUMNS = listOf(
        LINKING_ID,
        SRC_ENTITY_SET_ID,
        SRC_ENTITY_KEY_ID,
        DST_ENTITY_SET_ID,
        DST_ENTITY_KEY_ID,
        SCORE
).joinToString(",", transform = PostgresColumnDefinition::getName)

private val CLUSTER_CONTAINING_SQL = "SELECT * FROM ${MATCHED_ENTITIES.name} WHERE linking_id = ANY(?)"

private val DELETE_SQL = "DELETE FROM ${MATCHED_ENTITIES.name} " +
        "WHERE ${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ? " +
        "AND ${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ?"

private val DELETE_NEIGHBORHOOD_SQL = "DELETE FROM ${MATCHED_ENTITIES.name} " +
        "WHERE (${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ?) " +
        "OR (${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} = ?)"

private val DELETE_NEIGHBORHOODS_SQL = "DELETE FROM ${MATCHED_ENTITIES.name} WHERE " +
        "(${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] ))) OR " +
        "(${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] ))) "


private val NEIGHBORHOOD_SQL = "SELECT * FROM ${MATCHED_ENTITIES.name} " +
        "WHERE (${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} = ?) "

private val INSERT_SQL = "INSERT INTO ${MATCHED_ENTITIES.name} ($COLUMNS) VALUES (?,?,?,?,?,?) " +
        "ON CONFLICT ON CONSTRAINT matched_entities_pkey DO UPDATE SET ${SCORE.name} = EXCLUDED.${SCORE.name}"

private val BLOCKS_BY_AVG_SCORE_SQL =
        "SELECT  ${SRC_ENTITY_SET_ID.name} as entity_set_id, " +
                "${SRC_ENTITY_KEY_ID.name} as id, " +
                "avg(score) as $AVG_SCORE_FIELD " +
                "FROM ${MATCHED_ENTITIES.name} " +
                "GROUP BY (${SRC_ENTITY_SET_ID.name},${SRC_ENTITY_KEY_ID.name}) " +
                "ORDER BY $AVG_SCORE_FIELD ASC"

private val BLOCKS_BY_SIZE_SQL = "SELECT ${SRC_ENTITY_SET_ID.name} as entity_set_id, $SRC_ENTITY_KEY_ID as id, count(*) as $BLOCK_SIZE_FIELD" +
        "FROM ${MATCHED_ENTITIES.name} " +
        "GROUP BY (${SRC_ENTITY_SET_ID.name},${SRC_ENTITY_KEY_ID.name}) " +
        "ORDER BY $BLOCK_SIZE_FIELD DESC"

private val UPDATE_LINKED_ENTITIES_SQL = "UPDATE ${IDS.name} " +
        "SET ${LINKING_ID.name} = ?, ${LAST_LINK.name}=now() WHERE ${ENTITY_SET_ID.name} =? AND ${ID_VALUE.name}=?"

private val ENTITY_KEY_IDS_NEEDING_LINKING = "SELECT ${ENTITY_SET_ID.name},${ID.name} " +
        "FROM ${IDS.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ANY(?) AND ${LAST_LINK.name} < ${LAST_WRITE.name} AND ( ${LAST_INDEX.name} >= ${LAST_WRITE.name}) " +
        "AND ${VERSION.name} > 0 LIMIT ?"

private val ENTITY_KEY_IDS_NOT_LINKED = "SELECT ${ENTITY_SET_ID.name},${ID.name} " +
        "FROM ${IDS.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ANY(?) AND ${LAST_LINK.name} < ${LAST_WRITE.name} " +
        "AND ${VERSION.name} > 0 LIMIT ?"

private val LINKABLE_ENTITY_SET_IDS = "SELECT ${ID.name} " +
        "FROM ${ENTITY_SETS.name} " +
        "WHERE ${ENTITY_TYPE_ID.name} = ANY(?) AND NOT ${ID.name} = ANY(?) AND ${ID.name} = ANY(?) "
