package com.openlattice.postgres

import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*


/**
 * This function prepares a statement for the requested query, prepended with a CTE that acquires
 * locks on the [IDS] table, executes the query in batches per partition, and returns the number
 * of rows updated.
 *
 * This should only wrap queries that update existing rows in the [IDS] table, and should operate on
 * a single partition of a single entity set.
 *
 * @param connection The Hikari connection that will be used to execute the transaction
 * @param query The SQL query that will be executed once the locks have been acquired
 * @param entitySetId The entity set id that will be updated
 * @param entityKeyIdsByPartition A map from partition to a collection of entityKeyIds on that partition to
 * update. If the query operates on an entire entity set (as opposed to specific entity key ids) the values
 * in this map will be ignored.
 * @param shouldLockEntireEntitySet A boolean indicating whether the locks should operate on an entire entity
 * set, or whether they should apply a filter on the id values in [entityKeyIdsByPartition]. Defaults to false.
 * @param bindPreparedStatementFn A function that binds any remaining PreparedStatement parameters. This function
 * operates on the PreparedStatement, an Int (indicating the partition being operated on), and an Int (indicating
 * the current bind index).
 *
 * @return The number of rows that were updated.
 *
 */
fun lockIdsAndExecute(
        connection: Connection,
        entitySetId: UUID,
        partition: Int,
        entityKeyIds: Collection<UUID> = listOf(),
        execute: () -> Int
) : Int {
    require( !connection.autoCommit ) { "Connection must not be in autocommit mode." }

    val shouldLockEntireEntitySet = entityKeyIds.isEmpty()
    val lockSql = if (shouldLockEntireEntitySet) LOCKING_WITHOUT_IDS else LOCKING_WITH_IDS
    val lock = connection.prepareStatement(lockSql)

    return try {
            lock.setObject(1, entitySetId)
            lock.setInt(2, partition)
            if (!shouldLockEntireEntitySet) {
                lock.setArray(3, PostgresArrays.createUuidArray(connection, entityKeyIds))
            }

            // We set index to the last bound index so that the [bindPreparedStatementFn] can use
            // manual bind numbering added to this offset (which is 1-indexed)
            execute()
    } catch (ex: Exception) {
        connection.rollback()
        0
    }
}


fun getIdsByPartition(entityKeyIds: Collection<UUID>, partitions: List<Int>): Map<Int, List<UUID>> {
    return entityKeyIds.groupBy { getPartition(it, partitions) }
}

fun getPartitionMapForEntitySet(partitions: Collection<Int>): Map<Int, List<UUID>> {
    return partitions.associateWith { listOf<UUID>() }
}

private val LOCKING_WITH_IDS = "SELECT 1" +
        "  FROM ${IDS.name} " +
        "    WHERE ${ENTITY_SET_ID.name} = ? " +
        "    AND ${PARTITION.name} = ? " +
        "    AND ${ID.name} = ANY(?) " +
        "  ORDER BY ${ID.name} " +
        "  FOR UPDATE "

private val LOCKING_WITHOUT_IDS = "SELECT 1" +
        "  FROM ${IDS.name} " +
        "    WHERE ${ENTITY_SET_ID.name} = ? " +
        "    AND ${PARTITION.name} = ? " +
        "  ORDER BY ${ID.name} " +
        "  FOR UPDATE "