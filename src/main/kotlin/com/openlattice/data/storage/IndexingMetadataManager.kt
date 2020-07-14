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
import com.openlattice.postgres.getPartitionMapForEntitySet
import com.openlattice.postgres.lockIdsAndExecute
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
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(entityKeyIdsWithLastWrite.keys).mapValues { it.value.toList() }
        return entityKeyIdsWithLastWrite.map { (entitySetId, entities) ->

            val partitions = entitySetPartitions.getValue(entitySetId)
            val entitiesByPartition = entities.entries
                    .groupBy({ getPartition(it.key, partitions) }, { it.toPair() })
                    .mapValues { it.value.toMap() }

            lockIdsAndExecute(
                    hds.connection,
                    updateLastIndexSql,
                    entitySetId,
                    entitiesByPartition.mapValues { it.value.keys }
            ) { ps, partition, offset ->
                prepareIndexQuery(
                        ps,
                        entitySetId,
                        partition,
                        entitiesByPartition.getValue(partition),
                        offset
                )
            }
        }.sum()

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
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(linkingIdsWithLastWrite.keys).mapValues { it.value.toList() }

        return linkingIdsWithLastWrite.map { (entitySetId, entities) ->
            val partitions = entitySetPartitions.getValue(entitySetId)
            val entitiesByPartition = entities.entries
                    .groupBy({ getPartition(it.key, partitions) }, { it.toPair() })
                    .mapValues { it.value.toMap() }

            lockIdsAndExecute(
                    hds.connection,
                    updateLastLinkingIndexSql,
                    entitySetId,
                    entitiesByPartition.mapValues { it.value.keys }
            ) { ps, partition, offset ->
                val mergedLinkingIdsWithLastWrite = entitiesByPartition.getValue(partition).values
                        .fold(mutableMapOf<UUID, OffsetDateTime>()) { acc, map ->
                            acc.putAll(map)
                            acc
                        }

                prepareIndexQuery(
                        ps,
                        entitySetId,
                        partition,
                        mergedLinkingIdsWithLastWrite,
                        offset
                )

            }
        }.sum()

    }

    private fun prepareIndexQuery(
            stmt: PreparedStatement,
            entitySetId: UUID,
            partition: Int,
            idsWithLastWrite: Map<UUID, OffsetDateTime>,
            startingIndex: Int
    ) {
        idsWithLastWrite.entries
                .groupBy { it.value }
                .mapValues { it.value.map { entry -> entry.key } }
                .forEach { (lastWrite, entities) ->
                    var index = startingIndex

                    stmt.setObject(index++, lastWrite)
                    stmt.setObject(index++, entitySetId)
                    stmt.setArray(index++, PostgresArrays.createUuidArray(stmt.connection, entities))
                    stmt.setInt(index, partition)

                    stmt.addBatch()
                }
    }

    /**
     * Sets the last_index/last_link_index of provided entities to current datetime. Used when un-indexing entities
     * after deletion.
     * @param entityKeyIds Map of (normal) entity set ids to entity key ids.
     */
    fun markAsUnIndexed(entityKeyIds: Map<UUID, Set<UUID>>): Int {
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(entityKeyIds.keys).mapValues { it.value.toList() }

        return entityKeyIds.map { (entitySetId, entityKeyIds) ->
            val partitions = entitySetPartitions.getValue(entitySetId)
            val idsByPartition = getIdsByPartition(entityKeyIds, partitions)

            lockIdsAndExecute(
                    hds.connection,
                    markLastIndexSql,
                    entitySetId,
                    idsByPartition
            ) { ps, partition, offset ->
                val idsArray = PostgresArrays.createUuidArray(ps.connection, idsByPartition.getValue(partition))

                ps.setObject(1 + offset, entitySetId)
                ps.setArray(2 + offset, idsArray)
                ps.setInt(3 + offset, partition)

            }
        }.sum()
    }

    /**
     * Sets the last_write of provided entity set to current datetime. Used when un-indexing entities after entity set
     * data deletion.
     * @param entitySetId The id of the (normal) entity set id.
     */
    fun markAsUnIndexed(entitySetId: UUID): Int {
        val partitions = partitionManager.getEntitySetPartitions(entitySetId)

        return lockIdsAndExecute(
                hds.connection,
                markEntitySetLastIndexSql,
                entitySetId,
                getPartitionMapForEntitySet(partitions),
                shouldLockEntireEntitySet = true
        ) { ps, partition, offset ->
            ps.setObject(1 + offset, entitySetId)
            ps.setInt(2 + offset, partition)
        }
    }


    fun markEntitySetsAsNeedsToBeIndexed(entitySetIds: Set<UUID>, linking: Boolean): Int {
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(entitySetIds)

        return entitySetIds.map { entitySetId ->
            lockIdsAndExecute(
                    hds.connection,
                    markEntitySetsAsNeedsToBeIndexedSql(linking),
                    entitySetId,
                    getPartitionMapForEntitySet(entitySetPartitions.getValue(entitySetId)),
                    shouldLockEntireEntitySet = true
            ) { ps, partition, offset ->
                ps.setObject(1 + offset, entitySetId)
                ps.setInt(2 + offset, partition)
            }
        }.sum()
    }

    fun markAsNeedsToBeLinked(normalEntityDataKeys: Set<EntityDataKey>): Int {
        val normalEntityKeys = normalEntityDataKeys
                .groupBy { it.entitySetId }
                .mapValues { it.value.map { edk -> edk.entityKeyId } }
        val entitySetPartitions = partitionManager.getPartitionsByEntitySetId(normalEntityKeys.keys).mapValues { it.value.toList() }

        return normalEntityKeys.map { (entitySetId, entityKeyIds) ->

            val partitions = entitySetPartitions.getValue(entitySetId)
            val idsByPartition = getIdsByPartition(entityKeyIds, partitions)

            lockIdsAndExecute(
                    hds.connection,
                    markAsNeedsToBeLinkedSql,
                    entitySetId,
                    idsByPartition
            ) { ps, partition, offset ->
                val normalEntityKeyIdsArray = PostgresArrays.createUuidArray(ps.connection, idsByPartition.getValue(partition))
                ps.setObject(1 + offset, entitySetId)
                ps.setArray(2 + offset, normalEntityKeyIdsArray)
                ps.setInt(3 + offset, partition)
            }
        }.sum()

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
 * 2. partition
 */
private val entitySetPartition =
        " ${ENTITY_SET_ID.name} = ? " +
        "AND ${PARTITION.name} = ? "


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
        "UPDATE ${IDS.name} SET ${LAST_LINK.name} = '-infinity()' WHERE ${VERSION.name} > 0 AND $entityKeyIdsInEntitySet"

// @formatter:on
