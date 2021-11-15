package com.openlattice.postgres

import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
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
 * @param entityKeyIdss A collection of entity key ids to operate on
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
        entityKeyIds: SortedSet<UUID> = sortedSetOf(),
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
            lockEntitySet(lock, entitySetId)
        } else {
            batchLockIds(lock, entitySetId, entityKeyIds)
        }

        val lockCount = lock.executeBatch().sum()
        logger.debug("Locked $lockCount entity key ids for entity set $entitySetId ")
        execute()
    } catch (ex: Exception) {
        connection.rollback()
        0
    }
}

private fun lockEntitySet(lock: PreparedStatement, entitySetId: UUID) {
    lock.setObject(1, entitySetId)
}

private fun batchLockIds(lock: PreparedStatement, entitySetId: UUID, entityKeyIds: Collection<UUID>) {
    entityKeyIds.sorted().forEach { entityKeyId ->
        lock.setObject(1, entitySetId)
        lock.setObject(2, entityKeyId)
        lock.addBatch()
    }
}

fun lockIdsAndExecute(
        connection: Connection,
        query: String,
        entitySetId: UUID,
        entityKeyIds: SortedSet<UUID>,
        shouldLockEntireEntitySet: Boolean = false,
        batch: Boolean = false,
        execute: (PreparedStatement, Collection<UUID>) -> Unit
): Int {

    if (entityKeyIds.isEmpty() && !shouldLockEntireEntitySet) {
        return 0
    }
    val ac = connection.autoCommit
    connection.autoCommit = false
    val lockSql = if (shouldLockEntireEntitySet) LOCK_ENTITY_SET_PARTITION else LOCKING_WITH_ID
    val lock = connection.prepareStatement(lockSql)
    val ps = connection.prepareStatement(query)
    return try {


            if (shouldLockEntireEntitySet) {
                lockEntitySet(lock, entitySetId)
            } else {
                batchLockIds(lock, entitySetId, entityKeyIds)
            }

            val lockCount = lock.executeBatch().sum()
            logger.debug("Locked $lockCount entity key ids for entity set $entitySetId")

            execute(ps, entityKeyIds)

            if (batch) {
                ps.addBatch()
            }

        val count = if (batch) ps.executeBatch().sum() else ps.executeUpdate()
        connection.commit()
        connection.autoCommit = ac
        count
    } catch (ex: Exception) {
        connection.rollback()
        throw ex
    }
}

private val LOCK_ENTITY_SET_PARTITION = "SELECT 1" +
        "  FROM ${IDS.name} " +
        "    WHERE ${ENTITY_SET_ID.name} = ? " +
        "  ORDER BY ${ID.name} " +
        "  FOR UPDATE "

private val LOCKING_WITH_ID = "SELECT 1" +
        "  FROM ${IDS.name} " +
        "    WHERE ${ENTITY_SET_ID.name} = ? " +
        "    AND ${ID.name} = ? " +
        "  ORDER BY ${ID.name} " +
        "  FOR UPDATE "
