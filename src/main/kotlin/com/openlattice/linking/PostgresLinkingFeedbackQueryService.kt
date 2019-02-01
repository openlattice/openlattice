package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.SRC_ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.SRC_ENTITY_KEY_ID
import com.openlattice.postgres.PostgresColumn.DST_ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.DST_ENTITY_KEY_ID
import com.openlattice.postgres.PostgresColumn.LINKED
import com.openlattice.postgres.PostgresTable.LINKING_FEEDBACK
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

const val FETCH_SIZE = 100000

class PostgresLinkingFeedbackQueryService(private val hds: HikariDataSource) {

    private val entityKeyComparator = Comparator<EntityDataKey> { key1, key2 ->
        if (key1.entitySetId != key2.entitySetId) {
            key1.entitySetId.compareTo(key2.entitySetId)
        } else {
            key1.entityKeyId.compareTo(key2.entityKeyId)
        }
    }

    fun addLinkingFeedback(entityLinkingFeedback: EntityLinkingFeedback): Int {
        return hds.connection.use {
            // Insert a feedback with always the same order to avoid conflicting values
            val orderedEntityPair =
                    sortedSetOf(entityKeyComparator, entityLinkingFeedback.src, entityLinkingFeedback.dst)

            val stmt = it.prepareStatement(INSERT_SQL)
            stmt.setObject(1, orderedEntityPair.first().entitySetId)
            stmt.setObject(2, orderedEntityPair.first().entityKeyId)
            stmt.setObject(3, orderedEntityPair.last().entitySetId)
            stmt.setObject(4, orderedEntityPair.last().entityKeyId)
            stmt.setObject(5, entityLinkingFeedback.linked)
            stmt.setObject(6, entityLinkingFeedback.linked)

            stmt.executeUpdate()
        }
    }

    fun getLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(SELECT_ALL_SQL)
                    stmt.fetchSize = FETCH_SIZE
                    val rs = stmt.executeQuery()

                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EntityLinkingFeedback> {
                    ResultSetAdapters.entityLinkingFeedback(it)
                }
        )
    }

    fun getLinkingFeedback(entityPair: Pair<EntityDataKey, EntityDataKey>): EntityLinkingFeedback? {
        val connection = hds.connection
        val stmt = connection.prepareStatement(SELECT_SQL)
        // We insert in order, so when selecting, we only have to check in order too
        val orderedEntityPair = sortedSetOf(entityKeyComparator, entityPair.first, entityPair.second)
        stmt.setObject(1, orderedEntityPair.first().entitySetId)
        stmt.setObject(2, orderedEntityPair.first().entityKeyId)
        stmt.setObject(3, orderedEntityPair.last().entitySetId)
        stmt.setObject(4, orderedEntityPair.last().entityKeyId)
        val rs = stmt.executeQuery()

        return if (rs.next()) {
            ResultSetAdapters.entityLinkingFeedback(rs)
        } else {
            null
        }
    }

    fun deleteLinkingFeedbacks(entitySetId: UUID, entityKeyIds: Set<UUID>): Int {
        return hds.connection.use {
            val stmt = it.prepareStatement(DELETE_SQL)

            val arr = PostgresArrays.createUuidArray(it, entityKeyIds)
            stmt.setObject(1, entitySetId)
            stmt.setArray(2, arr)
            stmt.setObject(3, entitySetId)
            stmt.setArray(4, arr)

            stmt.executeUpdate()
        }
    }
}

private val SELECT_ALL_SQL = "SELECT * FROM ${LINKING_FEEDBACK.name}"
private val SELECT_SQL = "SELECT * FROM ${LINKING_FEEDBACK.name} " +
        "WHERE ${LINKING_FEEDBACK.primaryKey.joinToString(" = ? AND ", "", " = ? ") { it.name }}"
private val INSERT_SQL = "INSERT INTO ${LINKING_FEEDBACK.name} VALUES(?,?,?,?,?) " +
        "ON CONFLICT ( ${LINKING_FEEDBACK.primaryKey.joinToString(",") { it.name }} ) " +
        "DO UPDATE SET ${LINKED.name} = ?"

private val DELETE_SQL = "DELETE FROM ${LINKING_FEEDBACK.name} WHERE " +
        "(${SRC_ENTITY_SET_ID.name} = ? AND ${SRC_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] ))) OR " +
        "(${DST_ENTITY_SET_ID.name} = ? AND ${DST_ENTITY_KEY_ID.name} IN (SELECT * FROM UNNEST( (?)::uuid[] ))) "