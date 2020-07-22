package com.openlattice.data.storage

import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.getIdsByPartition
import com.openlattice.postgres.lockIdsAndExecuteAndCommit
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.time.OffsetDateTime
import java.util.*


class IndexingMetadataManager(private val hds: HikariDataSource, private val partitionManager: PartitionManager) {

    /**
     * Marks entities as indexed by setting last_index = last_write.
     * @param entityKeyIdsWithLastWrite Map of (normal) entity_set_id to id to last_write.
     */
    fun markAsIndexed(
            entityKeyIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>> // entity_set_id -> id -> last_write
    ): Int {
        val entitySetPartitions = partitionManager
                .getPartitionsByEntitySetId(entityKeyIdsWithLastWrite.keys)
                .mapValues { it.value.toList() }
        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(updateLastIndexSql)
            entityKeyIdsWithLastWrite.map { (entitySetId, entities) ->
                val partitions = entitySetPartitions.getValue(entitySetId)
                entities.entries
                        .groupBy({ getPartition(it.key, partitions) }, { it.toPair() })
                        .toSortedMap()
                        .map { (partition, idsAndExpirations) ->
                            val idsAndExpirationsMap = idsAndExpirations.toMap()
                            prepareIndexQuery(
                                    ps,
                                    entitySetId,
                                    partition,
                                    idsAndExpirationsMap
                            )
                            ps.executeBatch().sum()
                        }.sum()
            }.sum()
        }

    }

    /**
     * Marks linking entities as indexed by setting last_index = last_write.
     * @param linkingIdsWithLastWrite Map of (normal) entity_set_id to origin id to linking_id to last_write.
     * @return The number of rows affected by this update: the number of normal entities associated to the provided
     * linking ids.
     */
    fun markLinkingEntitiesAsIndexed(
            linkingIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>
    ): Int {
        val entitySetPartitions = partitionManager
                .getPartitionsByEntitySetId(linkingIdsWithLastWrite.keys)
                .mapValues { it.value.toList() }

        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(updateLastLinkingIndexSql)
            linkingIdsWithLastWrite.map { (entitySetId, entities) ->
                val partitions = entitySetPartitions.getValue(entitySetId)
                entities.entries
                        .groupBy({ getPartition(it.key, partitions) }, { it.value })
                        .toSortedMap()
                        .map { (partition, idsAndExpiration) ->


                            val mergedLinkingIdsWithLastWrite = idsAndExpiration
                                    .fold(mutableMapOf<UUID, OffsetDateTime>()) { acc, map ->
                                        acc.putAll(map)
                                        acc
                                    }

                            prepareIndexQuery(
                                    ps,
                                    entitySetId,
                                    partition,
                                    mergedLinkingIdsWithLastWrite
                            )
                            ps.executeBatch().sum()
                        }.sum()
            }.sum()
        }

    }

    private fun prepareIndexQuery(
            stmt: PreparedStatement,
            entitySetId: UUID,
            partition: Int,
            idsWithLastWrite: Map<UUID, OffsetDateTime>
    ) {
        idsWithLastWrite
                .forEach { (id, lastWrite) ->
                    stmt.setObject(1, lastWrite)
                    stmt.setObject(2, entitySetId)
                    stmt.setObject(3, id)
                    stmt.setInt(4, partition)
                    stmt.addBatch()
                }
    }

    /**
     * Sets the last_index/last_link_index of provided entities to current datetime. Used when un-indexing entities
     * after deletion.
     * @param entityKeyIds Map of (normal) entity set ids to entity key ids.
     */
    fun markAsUnIndexed(entityKeyIds: Map<UUID, Set<UUID>>): Int {
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(
                entityKeyIds.keys
        ).mapValues { it.value.toList() }

        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(markLastIndexSql)
            entityKeyIds.map { (entitySetId, entityKeyIds) ->
                val partitions = entitySetPartitions.getValue(entitySetId)

                getIdsByPartition(entityKeyIds, partitions)
                        .toSortedMap()
                        .map { (partition, ids) ->

                            ids.forEach { id ->
                                ps.setObject(1, entitySetId)
                                ps.setObject(2, id)
                                ps.setInt(3, partition)
                                ps.addBatch()
                            }
                            ps.executeBatch().sum()
                        }.sum()
            }.sum()
        }
    }

    /**
     * Sets the last_write of provided entity set to current datetime. Used when un-indexing entities after entity set
     * data deletion.
     * @param entitySetId The id of the (normal) entity set id.
     */
    fun markAsUnIndexed(entitySetId: UUID): Int {
        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(markEntitySetLastIndexSql)
            partitionManager.getEntitySetPartitions(entitySetId).sorted().map { partition ->
                ps.setObject(1, entitySetId)
                ps.setInt(2, partition)
                ps.addBatch()
            }
            ps.executeBatch().sum()
        }
    }


    fun markEntitySetsAsNeedsToBeIndexed(entitySetIds: Set<UUID>, linking: Boolean): Int {
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(entitySetIds)
        val query = markEntitySetsAsNeedsToBeIndexedSql(linking)

        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(query)
            entitySetIds.map { entitySetId ->
                entitySetPartitions.getValue(entitySetId).sorted().map { partition ->
                    ps.setObject(1, entitySetId)
                    ps.setInt(2, partition)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }.sum()
        }
    }

    fun markAsNeedsToBeLinked(normalEntityDataKeys: Set<EntityDataKey>): Int {
        val normalEntityKeys = normalEntityDataKeys
                .groupBy { it.entitySetId }
                .mapValues { it.value.map { edk -> edk.entityKeyId } }
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(
                normalEntityKeys.keys
        ).mapValues { it.value.toList() }

        return hds.connection.use { connection ->
            val ps = connection.prepareStatement(markAsNeedsToBeLinkedSql)
            normalEntityKeys.map { (entitySetId, entityKeyIds) ->

                val partitions = entitySetPartitions.getValue(entitySetId)
                getIdsByPartition(entityKeyIds, partitions)
                        .toSortedMap()
                        .map { (partition, ids) ->
                            ids.forEach { id ->
                                ps.setObject(1, entitySetId)
                                ps.setObject(2, id)
                                ps.setInt(3, partition)
                                ps.addBatch()
                            }
                            ps.executeBatch().sum()
                        }.sum()
            }.sum()
        }
    }
}
// @formatter:off

/**
 * 1. entity set id
 * 2. entity key ids (uuid array)
 * 3. partition
 */
private val entityKeyIdsInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${ID.name} = ANY(?) " +
        "AND ${PARTITION.name} = ? "

/**
 * 1. entity set id
 * 2. entity key id
 * 3. partition
 */
private val entityKeyIdInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
                "AND ${ID.name} = ? " +
                "AND ${PARTITION.name} = ? "

/**
 * 1. entity set id
 * 2. linking ids (uuid array)
 * 3. partition
 */
private val linkingIdsInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${LINKING_ID.name} = ANY(?) " +
        "AND ${PARTITION.name} = ? "

/**
 * 1. entity set id
 * 2. linking id
 * 3. partition
 */
private val linkingIdInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
                "AND ${LINKING_ID.name} = ? " +
                "AND ${PARTITION.name} = ? "

/**
 * 1. entity set id
 * 2. partition
 */
private val entitySetPartition =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${PARTITION.name} = ? "


/**
 * Arguments of preparable sql in order:
 * 1. last index
 * 2. entity set id
 * 3. entity key id
 * 4. partition
 */
private val updateLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? WHERE $entityKeyIdInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. last linking index
 * 2. entity set id
 * 3. linking ids (uuid array)
 * 4. partition
 */
private val updateLastLinkingIndexSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? WHERE $linkingIdInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. entity key ids (uuid array)
 * 3. partition
 */
private val markLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = 'now()' WHERE $entityKeyIdInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. partition
 */
private val markEntitySetLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = 'now()' WHERE $entitySetPartition"



/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. partition
 */
fun markEntitySetsAsNeedsToBeIndexedSql(linking: Boolean): String {
    val updateColumn = if (linking) LAST_LINK_INDEX.name else LAST_INDEX.name

    return "UPDATE ${IDS.name} SET $updateColumn = '-infinity()' " +
            "WHERE ${ENTITY_SET_ID.name} = ? AND ${PARTITION.name} = ?"
}

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. normal entity key ids (uuid array)
 * 3. partition
 */
private val markAsNeedsToBeLinkedSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' WHERE ${VERSION.name} > 0 AND $entityKeyIdInEntitySet"

// @formatter:on
