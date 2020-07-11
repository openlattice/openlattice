package com.openlattice.postgres

import com.geekbeast.util.LinearBackoff
import com.geekbeast.util.attempt
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.lockEntitiesInIdsTable
import org.postgresql.util.PSQLException
import java.sql.Connection

class RetryableLockedIdsOperator {

    companion object {

        private const val MAX_INTERVAL: Long = 60 * 1_000
        private const val RETRY_OFFSET: Long = 125
        private const val MAX_RETRY_ATTEMPTS = 32

        fun lockIdsAndExecute(
                connection: Connection,
                entitySetIdToEntityKeyId: Map<EntityDataKey, Int>,
                operateOnConnectionFn: (Connection) -> Int
        ): Int {

            return attempt(LinearBackoff(MAX_INTERVAL, RETRY_OFFSET), MAX_RETRY_ATTEMPTS) {

                connection.use { conn ->

                    conn.autoCommit = false
                    val lockEntities = conn.prepareStatement(lockEntitiesInIdsTable)

                    try {
                        entitySetIdToEntityKeyId.entries.sortedBy { it.key.entityKeyId }.forEach {

                            lockEntities.setObject(1, it.key.entitySetId)
                            lockEntities.setObject(2, it.key.entityKeyId)
                            lockEntities.setInt(3, it.value)
                            lockEntities.execute()
                        }

                        val numUpdates = operateOnConnectionFn(conn)

                        conn.commit()

                        numUpdates
                    } catch (ex: PSQLException) {
                        //Should be pretty rare.
                        conn.rollback()
                        throw ex
                    }
                }
            }

        }
    }
}