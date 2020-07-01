package com.openlattice.data.storage

import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_LINK
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
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
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(entityKeyIdsWithLastWrite.keys)

        return hds.connection.use { connection ->

            connection.prepareStatement(updateLastIndexSql).use { stmt ->

                entityKeyIdsWithLastWrite.forEach { (entitySetId, entities) ->

                    val partitions = entitySetPartitions.getValue(entitySetId).toList()
                    entities.entries
                            .groupBy({
                                         getPartition(
                                                 it.key, partitions
                                         )
                                     }, { it.toPair() })
                            .mapValues { it.value.toMap() }
                            .forEach { (partition, entitiesWithLastWrite) ->

                                prepareIndexQuery(
                                        connection,
                                        stmt,
                                        entitySetId,
                                        partition,
                                        entitiesWithLastWrite
                                )
                            }
                }
                stmt.executeBatch().sum()
            }
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
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(linkingIdsWithLastWrite.keys)

        return hds.connection.use { connection ->

            connection.prepareStatement(updateLastLinkingIndexSql).use { stmt ->

                linkingIdsWithLastWrite.forEach { (entitySetId, entities) ->

                    val partitions = entitySetPartitions.getValue(entitySetId).toList()
                    entities.entries
                            .groupBy({
                                         getPartition(
                                                 it.key, partitions
                                         )
                                     }, { it.toPair() })
                            .mapValues { it.value.toMap() }
                            .forEach { (partition, linkingIdsByOriginId) ->

                                val mergedLinkingIdsWithLastWrite = linkingIdsByOriginId.values
                                        .fold(mutableMapOf<UUID, OffsetDateTime>()) { acc, map ->
                                            acc.putAll(map)
                                            acc
                                        }

                                prepareIndexQuery(
                                        connection,
                                        stmt,
                                        entitySetId,
                                        partition,
                                        mergedLinkingIdsWithLastWrite
                                )
                            }
                }
                stmt.executeBatch().sum()
            }
        }
    }

    private fun prepareIndexQuery(
            connection: Connection,
            stmt: PreparedStatement,
            entitySetId: UUID,
            partition: Int,
            idsWithLastWrite: Map<UUID, OffsetDateTime>
    ) {
        idsWithLastWrite.entries
                .groupBy { it.value }
                .mapValues { it.value.map { it.key } }
                .forEach { (lastWrite, entities) ->

                    val idsArray = PostgresArrays.createUuidArray(connection, entities)
                    stmt.setObject(1, lastWrite)
                    stmt.setObject(2, entitySetId)
                    stmt.setArray(3, idsArray)
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
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(entityKeyIds.keys)

        return hds.connection.use { connection ->
            val updateSql = markLastIndexSql

            connection.prepareStatement(updateSql).use { stmt ->

                entityKeyIds.forEach { (entitySetId, entities) ->

                    val partitions = entitySetPartitions.getValue(entitySetId).toList()

                    entities.groupBy {
                        getPartition(
                                it, partitions
                        )
                    }
                            .forEach { (partition, entities) ->

                                val idsArray = PostgresArrays.createUuidArray(connection, entities)
                                stmt.setObject(1, entitySetId)
                                stmt.setArray(2, idsArray)
                                stmt.setInt(3, partition)

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
        val partitions = partitionManager.getEntitySetPartitions(entitySetId)

        return hds.connection.use { connection ->
            connection.prepareStatement(markEntitySetLastIndexSql).use { stmt ->
                val partitionsArray = PostgresArrays.createIntArray(connection, partitions)
                stmt.setObject(1, entitySetId)
                stmt.setArray(2, partitionsArray)

                stmt.executeUpdate()
            }
        }
    }


    fun markEntitySetsAsNeedsToBeIndexed(entitySetIds: Set<UUID>, linking: Boolean): Int {
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(entitySetIds).values.flatten()

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

    fun markAsNeedsToBeLinked(normalEntityDataKeys: Set<EntityDataKey>): Int {
        val normalEntityKeys = normalEntityDataKeys
                .groupBy { it.entitySetId }
                .mapValues {
                    it.value.map { it.entityKeyId }
                }
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(normalEntityKeys.keys)

        hds.connection.use { connection ->
            connection.prepareStatement(markAsNeedsToBeLinkedSql).use { stmt ->
                normalEntityKeys.forEach { (entitySetId, normalIds) ->
                    val partitions = entitySetPartitions.getValue(entitySetId).toList()

                    normalIds.groupBy {
                        getPartition(
                                it, partitions
                        )
                    }
                            .forEach { (partition, normalEntityKeyIds) ->
                                val normalEntityKeyIdsArray = PostgresArrays.createUuidArray(connection, normalEntityKeyIds)
                                stmt.setObject(1, entitySetId)
                                stmt.setArray(2, normalEntityKeyIdsArray)
                                stmt.setInt(3, partition)

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
 */
private val entityKeyIdsInEntitySet =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${ID.name} = ANY(?) " +
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
 * 2. partitions (int array)
 */
private val entitySet =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${PARTITION.name} = ANY(?) "


/**
 * Arguments of preparable sql in order:
 * 1. last index
 * 2. entity set id
 * 3. entity key ids (uuid array)
 * 4. partition
 */
private val updateLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? WHERE $entityKeyIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. last linking index
 * 2. entity set id
 * 3. linking ids (uuid array)
 * 4. partition
 */
private val updateLastLinkingIndexSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? WHERE $linkingIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. entity key ids (uuid array)
 * 3. partition
 */
private val markLastIndexSql = "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = 'now()' WHERE $entityKeyIdsInEntitySet"

/**
 * Arguments of preparable sql in order:
 * 1. entity set id
 * 2. partitions (int array)
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
 * 2. normal entity key ids (uuid array)
 * 3. partition
 */
private val markAsNeedsToBeLinkedSql =
        "UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' WHERE ${VERSION.name} > 0 AND $entityKeyIdsInEntitySet"

// @formatter:on
