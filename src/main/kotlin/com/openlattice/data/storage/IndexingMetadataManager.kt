package com.openlattice.data.storage

import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.*


class IndexingMetadataManager(private val hds: HikariDataSource, private val partitionManager: PartitionManager) {

    fun markAsIndexed(entityKeyIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>>): Int {
        var count = 0

        hds.connection.use { connection ->
            entityKeyIdsWithLastWrite.forEach { (entitySetId, entities) ->
                val partitions = partitionManager.getEntitySetPartitionsInfo(entitySetId).partitions.toList()

                entities.entries
                        .groupBy({ getPartition(it.key, partitions) }, { it.toPair() })
                        .mapValues { it.value.toMap() }
                        .forEach { (partition, entitiesWithLastWrite) ->

                            entitiesWithLastWrite.entries
                                    .groupBy { it.value }
                                    .mapValues { it.value.map { it.key } }
                                    .forEach { (lastWrite, entities) ->
                                        count += markAsIndexed(connection, entitySetId, entities, partition, lastWrite)
                                    }
                        }
            }
        }
        return count
    }

    private fun markAsIndexed(
            connection: Connection,
            entitySetId: UUID,
            entityKeyIds: List<UUID>,
            partition: Int,
            lastWrite: OffsetDateTime
    ): Int {
        return connection.prepareStatement(updateLastIndexSql).use { stmt ->
            val entitiesArray = PostgresArrays.createUuidArray(connection, entityKeyIds)
            stmt.setObject(1, lastWrite)
            stmt.setObject(2, entitySetId)
            stmt.setArray(3, entitiesArray)
            stmt.setInt(4, partition)
            stmt.executeUpdate()
        }
    }

    fun markLinkingIdsAsIndexed(linkingEntityKeyIdsWithLastWrite: Map<UUID, Map<UUID, OffsetDateTime>>): Int {
        return hds.connection.use { connection ->
            val entitySetPartitions = partitionManager
                    .getEntitySetsPartitionsInfo(linkingEntityKeyIdsWithLastWrite.keys)

            connection.prepareStatement(updateLastLinkingIndexSql).use { stmt ->
                linkingEntityKeyIdsWithLastWrite.forEach { (entitySetId, linkingEntities) ->
                    val partitions = entitySetPartitions.getValue(entitySetId).partitions
                    val partitionsArray = PostgresArrays.createIntArray(connection, partitions)

                    linkingEntities.entries
                            .groupBy { it.value }
                            .mapValues { it.value.map { it.key } }
                            .forEach { (lastWrite, linkingIds) ->
                                val linkingIdsArray = PostgresArrays.createUuidArray(connection, linkingIds)

                                stmt.setObject(1, lastWrite)
                                stmt.setObject(2, entitySetId)
                                stmt.setArray(3, linkingIdsArray)
                                stmt.setArray(4, partitionsArray)
                                stmt.addBatch()
                            }
                }

                return stmt.executeBatch().sum()
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

                return stmt.executeUpdate()
            }
        }
    }

    fun markLinkingIdsAsNeedToBeIndexed(linkingEntityKeys: Map<UUID, Set<UUID>>): Int {
        val entitySetPartitions = partitionManager.getEntitySetsPartitionsInfo(linkingEntityKeys.keys)

        hds.connection.use { connection ->
            connection.prepareStatement(markLinkingIdsAsNeedToBeIndexedSql).use { stmt ->
                linkingEntityKeys.forEach { (entitySetId, linkingIds) ->
                    val linkingIdsArray = PostgresArrays.createUuidArray(connection, linkingIds)
                    val partitionsArray = PostgresArrays
                            .createIntArray(connection, entitySetPartitions.getValue(entitySetId).partitions)
                    stmt.setObject(1, entitySetId)
                    stmt.setArray(2, partitionsArray)
                    stmt.setArray(3, linkingIdsArray)

                    stmt.addBatch()
                }

                return stmt.executeBatch().sum()
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
                    val partitionsArray = PostgresArrays
                            .createIntArray(connection, entitySetPartitions.getValue(entitySetId).partitions)
                    val linkingIdsArray = PostgresArrays.createUuidArray(connection, linkingIds)
                    stmt.setObject(1, entitySetId)
                    stmt.setArray(2, partitionsArray)
                    stmt.setArray(3, linkingIdsArray)
                    stmt.addBatch()
                }

                return stmt.executeBatch().sum()
            }
        }
    }
}

/**
 * Arguments of preparable sql in order:
 * 1. last index
 * 2. entity set id
 * 3. entity key ids (uuid array)
 * 4. partition
 */
private val updateLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? " +
        "WHERE ${VERSION.name} > 0 AND ${ENTITY_SET_ID.name} = ? AND ${ID.name} = ANY(?) AND ${PARTITION.name} = ?"

/**
 * Arguments of preparable sql in order:
 * 1. last linking index
 * 2. entity set id
 * 3. linking ids (uuid array)
 * 4. partitions (int array)
 */
private val updateLastLinkingIndexSql = "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? " +
        "WHERE ${VERSION.name} > 0 AND ${ENTITY_SET_ID.name} = ? AND ${LINKING_ID.name} = ANY(?) " +
        "AND ${LINKING_ID.name} IS NOT NULL AND ${PARTITION.name} = ANY(?)"


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
 * 2. partitions (int array)
 * 3. linking ids (uuid array)
 */
private val markLinkingIdsAsNeedToBeIndexedSql = "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = '-infinity()' " +
        "WHERE ${VERSION.name} > 0 AND ${ENTITY_SET_ID.name} = ? AND ${PARTITION.name} = ANY(?) " +
        "AND ${LINKING_ID.name} IS NOT NULL AND ${LINKING_ID.name} = ANY(?)"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. partitions (int array)
 * 3. linking ids (uuid array)
 */
private val markAsNeedsToBeLinkedSql = "UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' " +
        "WHERE ${VERSION.name} > 0 AND ${ENTITY_SET_ID.name} = ? AND ${PARTITION.name} = ANY(?) " +
        "AND ${LINKING_ID.name} IS NOT NULL AND ${LINKING_ID.name} = ANY(?)"

