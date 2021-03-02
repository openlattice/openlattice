package com.openlattice.postgres

import com.openlattice.data.storage.partitions.getPartition
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*

private class PostgresLockingUtils() {}

private val logger = LoggerFactory.getLogger(PostgresLockingUtils::class.java)

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
        shouldLockEntireEntitySet: Boolean = false,
        execute: () -> Int
): Int {
    require(!connection.autoCommit) { "Connection must not be in autocommit mode." }

    if (!shouldLockEntireEntitySet && entityKeyIds.isEmpty()) {
        return 0
    }

    val lockSql = if (shouldLockEntireEntitySet) LOCK_ENTITY_SET_PARTITION else LOCKING_WITH_ID
    val lock = connection.prepareStatement(lockSql)

    return try {
        if (shouldLockEntireEntitySet) {
            lockEntitySet(lock, entitySetId, partition)
        } else {
            batchLockIds(lock, entitySetId, partition, entityKeyIds)
        }

        val lockCount = lock.executeBatch().sum()
        logger.info("Locked $lockCount entity key ids for entity set $entitySetId and partition $partition")
        execute()
    } catch (ex: Exception) {
        connection.rollback()
        0
    }
}

private fun lockEntitySet(lock: PreparedStatement, entitySetId: UUID, partition: Int) {
    lock.setObject(1, entitySetId)
    lock.setInt(2, partition)
}

private fun batchLockIds(lock: PreparedStatement, entitySetId: UUID, partition: Int, entityKeyIds: Collection<UUID>) {
    entityKeyIds.sorted().forEach { entityKeyId ->
        lock.setObject(1, entitySetId)
        lock.setInt(2, partition)
        lock.setObject(3, entityKeyId)
        lock.addBatch()
    }
}

fun lockIdsAndExecute(
        connection: Connection,
        query: String,
        entitySetId: UUID,
        idsByPartition: Map<Int, Collection<UUID>>,
        shouldLockEntireEntitySet: Boolean = false,
        batch: Boolean = false,
        execute: (PreparedStatement, Int, Collection<UUID>) -> Unit
): Int {

    if (idsByPartition.isEmpty() && !shouldLockEntireEntitySet) {
        return 0
    }
    val ac = connection.autoCommit
    connection.autoCommit = false
    val lockSql = if (shouldLockEntireEntitySet) LOCK_ENTITY_SET_PARTITION else LOCKING_WITH_ID
    val lock = connection.prepareStatement(lockSql)
    val ps = connection.prepareStatement(query)
    return try {
        val updates = idsByPartition.toSortedMap().map { (partition, entityKeyIds) ->

            if (shouldLockEntireEntitySet) {
                lockEntitySet(lock, entitySetId, partition)
            } else {
                batchLockIds(lock, entitySetId, partition, entityKeyIds)
            }

            val lockCount = lock.executeBatch().sum()
            logger.info("Locked $lockCount entity key ids for entity set $entitySetId and partition $partition")

            execute(ps, partition, entityKeyIds)
            if (batch) {
                ps.addBatch()
                0
            } else {
                ps.executeUpdate()
            }
        }

        val count = if (batch) ps.executeBatch().sum() else updates.sum()
        connection.commit()
        connection.autoCommit = ac
        count
    } catch (ex: Exception) {
        connection.rollback()
        throw ex
    }
}

fun lockIdsAndExecuteAndCommit(
        hds: HikariDataSource,
        preparableQuery: String,
        entitySetId: UUID,
        partition: Int,
        entityKeyIds: Collection<UUID> = listOf(),
        shouldLockEntireEntitySet: Boolean = false,
        bind: (PreparedStatement) -> Unit
): Int {
    if (!shouldLockEntireEntitySet && entityKeyIds.isEmpty()) {
        return 0
    }

    return hds.connection.use { connection ->

        connection.autoCommit = false
        val lockSql = if (shouldLockEntireEntitySet) LOCK_ENTITY_SET_PARTITION else LOCKING_WITH_ID
        val lock = connection.prepareStatement(lockSql)

        try {
            if (shouldLockEntireEntitySet) {
                lockEntitySet(lock, entitySetId, partition)
            } else {
                batchLockIds(lock, entitySetId, partition, entityKeyIds)
            }

            val lockCount = lock.executeBatch().sum()
            logger.info("Locked $lockCount entity key ids for entity set $entitySetId and partition $partition")

            val ps = connection.prepareStatement(preparableQuery)
            bind(ps)
            val updateCount = ps.executeUpdate()

            connection.commit()
            connection.autoCommit = true
            return@use updateCount
        } catch (ex: Exception) {
            connection.rollback()
            throw ex
        }
    }
}

fun getIdsByPartition(entityKeyIds: Collection<UUID>, partitions: List<Int>): Map<Int, List<UUID>> {
    return entityKeyIds.groupBy { getPartition(it, partitions) }
}

private val LOCK_ENTITY_SET_PARTITION = "SELECT 1" +
        "  FROM ${IDS.name} " +
        "    WHERE ${ENTITY_SET_ID.name} = ? " +
        "    AND ${PARTITION.name} = ? " +
        "  ORDER BY ${ID.name} " +
        "  FOR UPDATE "

private val LOCKING_WITH_ID = "SELECT 1" +
        "  FROM ${IDS.name} " +
        "    WHERE ${ENTITY_SET_ID.name} = ? " +
        "    AND ${PARTITION.name} = ? " +
        "    AND ${ID.name} = ? " +
        "  ORDER BY ${ID.name} " +
        "  FOR UPDATE "
