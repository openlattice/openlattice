package com.openlattice.subscriptions

import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.authorization.Principal
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.SUBSCRIPTIONS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

class PostgresSubscriptionService(
        private val hds: HikariDataSource,
        private val mapper: ObjectMapper
) : SubscriptionService {

    // v code v

    override fun createOrUpdateSubscription(subscription: NeighborhoodQuery, user: Principal) {
        hds.connection.use { conn ->
            subscription.ids.map { ekid ->
                conn.prepareStatement(updateSubscriptionSQL).use {ps ->
                    ps.setObject(1, mapper.writeValueAsString(subscription.srcSelections))
                    ps.setObject(2, mapper.writeValueAsString(subscription.dstSelections))
                    ps.setObject(3, user.id)
                    ps.setObject(4, ekid)
                    ps.executeUpdate()
                }
            }
        }
    }

    override fun deleteSubscription(ekId: UUID, user: Principal) {
        hds.connection.use { conn ->
            val ps = conn.prepareStatement( deleteSubscriptionSQL )
            ps.setObject(1, user.id)
            ps.setObject(2, ekId )
            ps.executeUpdate()
        }
    }

    override fun getAllSubscriptions(user: Principal): Iterable<NeighborhoodQuery> {
        return execSqlSelectReturningIterable(getAllSubscriptionsSQL,
                { rs: ResultSet ->  ResultSetAdapters.subscription( rs ) },
                { ps: PreparedStatement, conn: Connection ->
                    ps.setObject(1, user.id)
                    ps
                }
        )
    }

    override fun getSubscriptions(ekIds: List<UUID>, user: Principal): Iterable<NeighborhoodQuery> {
        return execSqlSelectReturningIterable(getSubscriptionSQL,
                { rs: ResultSet ->  ResultSetAdapters.subscription( rs ) },
                { ps: PreparedStatement, conn: Connection ->
                    val arr = PostgresArrays.createUuidArray( conn, ekIds )
                    ps.setObject(1, user.id)
                    ps.setObject(2, arr)
                    ps
                }
        )
    }

    // ^ code ^
    // v util v

    fun <T> execSqlSelectReturningIterable(
            sql: String,
            resultMappingFunc: (rs: ResultSet) -> T,
            statementFunction: (ps: PreparedStatement, conn: Connection) -> PreparedStatement = { ps: PreparedStatement, conn: Connection -> ps }
    ) : Iterable<T>{
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    connection.autoCommit = false
                    val ps = statementFunction( connection.prepareStatement(sql), connection)
                    val rs = ps.executeQuery()
                    StatementHolder(connection, ps, rs)
                },
                Function<ResultSet, T> { rs -> resultMappingFunc(rs) }
        )
    }

}


//private val addSubscriptionSQL = "INSERT INTO ${SUBSCRIPTIONS.name} " +
//        "(${PRINCIPAL_ID.name}, ${ID.name}, ${SRC_SELECTS.name}, ${DST_SELECTS.name})" +
//        " VALUES (?,?::uuid,?::jsonb,?::jsonb)"
private val updateSubscriptionSQL = "UPDATE ${SUBSCRIPTIONS.name} SET ${SRC_SELECTS.name} = ?::jsonb, ${DST_SELECTS.name} = ?::jsonb WHERE ${PRINCIPAL_ID.name} = ? AND ${ID.name} = ?::uuid"
private val createOrUpdateSubscriptionSQL = updateSubscriptionSQL
private val deleteSubscriptionSQL = "DELETE FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ? AND ${ID.name} = ?"
private val getSubscriptionSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ? AND ${ID.name} = ANY(?)"
private val getAllSubscriptionsSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ?"
