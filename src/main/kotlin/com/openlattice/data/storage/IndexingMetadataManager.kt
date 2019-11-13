package com.openlattice.data.storage

import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.zaxxer.hikari.HikariDataSource
import java.time.OffsetDateTime
import java.util.*


class IndexingMetadataManager(private val hds: HikariDataSource, private val partitionManager: PartitionManager) {

    fun markAsIndexed(
            entityKeyIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>>, // entity_set_id -> id -> last_write
            linking: Boolean
    ): Int {
        val entitySetPartitions = partitionManager.getEntitySetsPartitionsInfo(entityKeyIdsWithLastWrite.keys)

        return hds.connection.use { connection ->
            val updateSql = if (linking) updateLastLinkingIndexSql else updateLastIndexSql

            connection.prepareStatement(updateSql).use { stmt ->

                entityKeyIdsWithLastWrite.forEach { (entitySetId, entities) ->

                    val partitionsInfo = entitySetPartitions.getValue(entitySetId)
                    val partitions = partitionsInfo.partitions.toList()
                    val partitionVersion = partitionsInfo.partitionsVersion
                    entities.entries
                            .groupBy({ getPartition(it.key, partitions) }, { it.toPair() })
                            .mapValues { it.value.toMap() }
                            .forEach { (partition, entitiesWithLastWrite) ->

                                entitiesWithLastWrite.entries
                                        .groupBy { it.value }
                                        .mapValues { it.value.map { it.key } }
                                        .forEach { (lastWrite, entities) ->

                                            val idsArray = PostgresArrays.createUuidArray(connection, entities)
                                            stmt.setObject(1, lastWrite)
                                            stmt.setObject(2, entitySetId)
                                            stmt.setArray(3, idsArray)
                                            stmt.setInt(4, partition)
                                            stmt.setInt(5, partitionVersion)

                                            stmt.addBatch()
                                        }
                            }
                }
                stmt.executeBatch().sum()
            }
        }
    }

    /**
     * Sets the last_index/last_link_index of provided entities to current datetime. Used when un-indexing entities
     * after deletion.
     * @param entityKeyIds Map of (normal) entity set ids and either entity key ids or linking ids, depending on
     * [linking].
     * @param linking Denotes, if the provided ids are linking ids or not.
     */
    fun markAsUnIndexed(entityKeyIds: Map<UUID, Set<UUID>>, linking: Boolean): Int {
        val entitySetPartitions = partitionManager.getEntitySetsPartitionsInfo(entityKeyIds.keys)

        return hds.connection.use { connection ->
            val updateSql = if (linking) markLastLinkingIndexSql else markLastIndexSql

            connection.prepareStatement(updateSql).use { stmt ->

                entityKeyIds.forEach { (entitySetId, entities) ->

                    val partitionsInfo = entitySetPartitions.getValue(entitySetId)
                    val partitions = partitionsInfo.partitions.toList()
                    val partitionVersion = partitionsInfo.partitionsVersion

                    entities.groupBy { getPartition(it, partitions) }
                            .forEach { (partition, entities) ->

                                val idsArray = PostgresArrays.createUuidArray(connection, entities)
                                stmt.setObject(1, entitySetId)
                                stmt.setArray(2, idsArray)
                                stmt.setInt(3, partition)
                                stmt.setInt(4, partitionVersion)

                                stmt.addBatch()
                            }
                }
                stmt.executeBatch().sum()
            }
        }
    }

    /**
     * Sets the last_write of provided entity set to current datetime. Used when un-indexing entities after entity set
     * data deletion.
     * @param entitySetId The id of the (normal) entity set id.
     */
    fun markAsUnIndexed(entitySetId: UUID): Int {
        val partitionInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
        val partitions = partitionInfo.partitions.toList()
        val partitionVersion = partitionInfo.partitionsVersion

        return hds.connection.use { connection ->
            connection.prepareStatement(markEntitySetLastIndexSql).use { stmt ->
                val partitionsArray = PostgresArrays.createIntArray(connection, partitions)
                stmt.setObject(1, entitySetId)
                stmt.setArray(2, partitionsArray)
                stmt.setInt(3, partitionVersion)

                stmt.executeUpdate()
            }
        }
    }


    fun markEntitySetsAsNeedsToBeIndexed(entitySetIds: Set<UUID>, linking: Boolean): Int {
        val entitySetPartitions = partitionManager.getEntitySetsPartitionsInfo(entitySetIds).values
                .flatMap { it.partitions }
                .toSet()

        return hds.connection.use { connection ->
            val updateSql = markEntitySetsAsNeedsToBeIndexedSql(linking)
            connection.prepareStatement(updateSql).use { stmt ->
                val entitySetIdsArray = PostgresArrays.createUuidArray(connection, entitySetIds)
                val partitionsArray = PostgresArrays.createIntArray(connection, entitySetPartitions)
                stmt.setArray(1, entitySetIdsArray)
                stmt.setArray(2, partitionsArray)

                stmt.executeUpdate()
            }
        }
    }

    fun markAsNeedsToBeLinked(linkingEntityDataKeys: Set<EntityDataKey>): Int {
        val linkingEntityKeys = linkingEntityDataKeys
                .groupBy { it.entitySetId }
                .mapValues {
                    it.value.map { it.entityKeyId }
                }
        val entitySetPartitions = partitionManager.getEntitySetsPartitionsInfo(linkingEntityKeys.keys)

        hds.connection.use { connection ->
            connection.prepareStatement(markAsNeedsToBeLinkedSql).use { stmt ->
                linkingEntityKeys.forEach { (entitySetId, linkingIds) ->
                    val partitionsInfo = entitySetPartitions.getValue(entitySetId)
                    val partitions = partitionsInfo.partitions.toList()
                    val partitionVersion = partitionsInfo.partitionsVersion

                    linkingIds.groupBy { getPartition(it, partitions) }
                            .forEach { (partition, linkingIds) ->
                                val linkingIdsArray = PostgresArrays.createUuidArray(connection, linkingIds)
                                stmt.setObject(1, entitySetId)
                                stmt.setArray(2, linkingIdsArray)
                                stmt.setInt(3, partition)
                                stmt.setInt(4, partitionVersion)

                                stmt.addBatch()
                            }
                }

                return stmt.executeBatch().sum()
            }
        }
    }
}
// @formatter:off

/**
 * 1. entity set id
 * 2. entity key ids (uuid array)
 * 3. partition
 * 4. partition version
 */
private val entityKeyIdsInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${ID.name} = ANY(?) " +
        "AND ${PARTITION.name} = ? " +
        "AND ${PARTITIONS_VERSION.name} = ? "

/**
 * 1. entity set id
 * 2. linking ids (uuid array)
 * 3. partition
 * 4. partition version
 */
private val linkingIdsInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${LINKING_ID.name} = ANY(?) " +
        "AND ${LINKING_ID.name} IS NOT NULL " +
        "AND ${PARTITION.name} = ? " +
        "AND ${PARTITIONS_VERSION.name} = ? "

/**
 * 1. entity set id
 * 2. partitions (int array)
 * 3. partition version
 */
private val entitySet =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${PARTITION.name} = ANY(?) " +
        "AND ${PARTITIONS_VERSION.name} = ? "


/**
 * Arguments of preparable sql in order:
 * 1. last index
 * 2. entity set id
 * 3. entity key ids (uuid array)
 * 4. partition
 * 5. partition version
 */
private val updateLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? WHERE $entityKeyIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. last linking index
 * 2. entity set id
 * 3. linking ids (uuid array)
 * 4. partition
 * 5. partition version
 */
private val updateLastLinkingIndexSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? WHERE $linkingIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. entity key ids (uuid array)
 * 3. partition
 * 4. partition version
 */
private val markLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = 'now()' WHERE $entityKeyIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. linking ids (uuid array)
 * 3. partition
 * 4. partition version
 */
private val markLastLinkingIndexSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = 'now()' WHERE $linkingIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 3. partitions (int array)
 * 4. partition version
 */
private val markEntitySetLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = 'now()' WHERE $entitySet"



/**
 * Arguments of preparable sql in order:
 * 1. entity set ids (uuid array)
 * 2. partitions (int array)
 */
fun markEntitySetsAsNeedsToBeIndexedSql(linking: Boolean): String {
    val updateColumn = if (linking) LAST_LINK_INDEX.name else LAST_INDEX.name

    return "UPDATE ${IDS.name} SET $updateColumn = '-infinity()' " +
            "WHERE ${ENTITY_SET_ID.name} = ANY(?) AND ${PARTITION.name} = ANY(?)"
}

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. entity key ids (uuid array)
 * 3. partition
 * 4. partition version
 */
private val markIdsAsNeedToBeIndexedSql =
        "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = '-infinity()' WHERE $entityKeyIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. linking ids (uuid array)
 * 3. partition
 * 4. partition version
 */
private val markAsNeedsToBeLinkedSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' WHERE ${VERSION.name} > 0 AND $linkingIdsInEntitySet"

// @formatter:on
