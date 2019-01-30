package com.openlattice.linking

import com.openlattice.postgres.PostgresTable.LINKING_FEEDBACK
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import java.sql.ResultSet
import java.util.function.Function
import java.util.function.Supplier

const val FETCH_SIZE = 100000

class PostgresLinkingFeedbackQueryService(
        private val hds: HikariDataSource
) {
    fun addLinkingFeedback(entityLinkingFeedback: EntityLinkingFeedback): Int {
        return hds.connection.use {
            // Insert a feedback with src-dst and dst-src too, so the primary key can hold up disregarding the order of
            // entities
            val stmt1 = it.prepareStatement(INSERT_SQL)
            stmt1.setObject(1, entityLinkingFeedback.src.entitySetId)
            stmt1.setObject(2, entityLinkingFeedback.src.entityKeyId)
            stmt1.setObject(3, entityLinkingFeedback.dst.entitySetId)
            stmt1.setObject(4, entityLinkingFeedback.dst.entityKeyId)
            stmt1.setObject(5, entityLinkingFeedback.linked)

            val stmt2 = it.prepareStatement(INSERT_SQL)
            stmt2.setObject(1, entityLinkingFeedback.dst.entitySetId)
            stmt2.setObject(2, entityLinkingFeedback.dst.entityKeyId)
            stmt2.setObject(3, entityLinkingFeedback.src.entitySetId)
            stmt2.setObject(4, entityLinkingFeedback.src.entityKeyId)
            stmt2.setObject(5, entityLinkingFeedback.linked)

            stmt1.executeUpdate() + stmt2.executeUpdate()
        }
    }

    fun getLinkingFeedbacks(): Iterable<EntityLinkingFeedback> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(SELECT_SQL)
                    stmt.fetchSize = FETCH_SIZE
                    val rs = stmt.executeQuery()

                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, EntityLinkingFeedback> {
                    ResultSetAdapters.entityLinkingFeedback(it)
                }
        )
    }
}

private val SELECT_SQL = "SELECT * FROM ${LINKING_FEEDBACK.name}"
private val INSERT_SQL = "INSERT INTO ${LINKING_FEEDBACK.name} VALUES(?,?,?,?,?)"