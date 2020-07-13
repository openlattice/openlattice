package com.openlattice.postgres

import com.openlattice.data.storage.lockEntitiesInIdsTable
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import org.postgresql.util.PSQLException
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*

class LockedIdsOperator {

    companion object {

        private const val MAX_INTERVAL: Long = 60 * 1_000
        private const val RETRY_OFFSET: Long = 125
        private const val MAX_RETRY_ATTEMPTS = 32

        fun lockIdsAndExecute(
                connection: Connection,
                query: String,
                entitySetId: UUID,
                entityKeyIdsByPartition: Map<Int, Collection<UUID>>,
                shouldLockEntireEntitySet: Boolean = false,
                bindPreparedStatementFn: (PreparedStatement, Int, Int) -> Unit
        ): Int {

            return connection.use { conn ->

                conn.autoCommit = false

                val lockingCTE = if (shouldLockEntireEntitySet) LOCKING_CTE_WITHOUT_IDS else LOCKING_CTE_WITH_IDS
                val ps = conn.prepareStatement("$lockingCTE $query")

                try {
                    entityKeyIdsByPartition.forEach { (partition, entityKeyIds) ->
                        var index = 1
                        ps.setObject(index++, entitySetId)
                        ps.setInt(index++, partition)
                        if (shouldLockEntireEntitySet) {
                            ps.setArray(index++, PostgresArrays.createUuidArray(conn, entityKeyIds))
                        }

                        bindPreparedStatementFn(ps, partition, index)
                        ps.addBatch()
                    }

                    val numUpdates = ps.executeBatch().sum()
                    conn.commit()

                    numUpdates
                } catch (ex: PSQLException) {
                    //Should be pretty rare.
                    conn.rollback()
                    throw ex
                }
            }


        }

        private val LOCKING_CTE_WITH_IDS = "WITH id_locks AS (" +
                "  SELECT 1" +
                "  FROM ${IDS.name} " +
                "    WHERE ${ENTITY_SET_ID.name} = ? " +
                "    AND ${PARTITION.name} = ? " +
                "    AND ${ID.name} = ANY(?) " +
                "  ORDER BY ${ID.name} " +
                ") "

        private val LOCKING_CTE_WITHOUT_IDS = "WITH id_locks AS (" +
                "  SELECT 1" +
                "  FROM ${IDS.name} " +
                "    WHERE ${ENTITY_SET_ID.name} = ? " +
                "    AND ${PARTITION.name} = ? " +
                "  ORDER BY ${ID.name} " +
                ") "
    }
}