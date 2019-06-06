package com.openlattice.subscriptions

import com.fasterxml.jackson.databind.ObjectMapper
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
    override fun addSubscription(subscription: NeighborhoodQuery): UUID {
        val newSubId = UUID.randomUUID()
        hds.connection.use { conn ->
            val ps = conn.prepareStatement(addSubscriptionSQL)
            ps.setObject(1, newSubId)
            ps.setObject(2, PostgresArrays.createUuidArray( conn, subscription.ids ) )
            ps.setObject(3, mapper.writeValueAsString(subscription.incomingSelections))
            ps.setObject(4, mapper.writeValueAsString(subscription.outgoingSelections))
            ps.executeUpdate()
        }
        return newSubId
    }

    override fun updateSubscription(subscription: Subscription): UUID {
        hds.connection.use { conn ->
            val ps = conn.prepareStatement(updateSubscriptionSQL)
            ps.setObject(1, subscription.query.ids)
            ps.setObject(2, subscription.query.incomingSelections)
            ps.setObject(3, subscription.query.outgoingSelections)
            ps.setObject(4, subscription.id)
            ps.executeUpdate()
        }
        return subscription.id
    }

    override fun deleteSubscription(subId: UUID) {
        hds.connection.use { conn ->
            val ps = conn.prepareStatement( deleteSubscriptionSQL )
            ps.setObject(1, subId )
            ps.executeUpdate()
        }
    }

    override fun getAllSubscriptions(): Iterable<Subscription> {
        return execSqlReturningIterable(getAllSubscriptionsSQL,
                { rs: ResultSet ->  ResultSetAdapters.subscription( rs ) }
        )
    }

    override fun getSubscriptions(subIds: List<UUID>): Iterable<Subscription> {
        return execSqlReturningIterable(getSubscriptionSQL,
                { rs: ResultSet ->  ResultSetAdapters.subscription( rs ) },
                { ps: PreparedStatement, conn: Connection ->
                    val arr = PostgresArrays.createUuidArray( conn, subIds )
                    ps.setObject(1, arr)
                    ps
                }
        )
    }

    fun <T> execSqlReturningIterable(
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

private val addSubscriptionSQL = "INSERT INTO ${SUBSCRIPTIONS.name} " +
        "(${ID.name}, ${ENTITY_KEY_IDS.name}, ${INCOMING_NEIGHBORHOOD_SELECTS.name}, ${OUTGOING_NEIGHBORHOOD_SELECTS.name})" +
        " VALUES (?,?,?,?)"
private val updateSubscriptionSQL = "UPDATE ${SUBSCRIPTIONS.name} SET ${ENTITY_KEY_IDS.name} = ?, ${INCOMING_NEIGHBORHOOD_SELECTS.name} = ?, ${OUTGOING_NEIGHBORHOOD_SELECTS.name} = ? WHERE ${ID.name} = ?"
private val deleteSubscriptionSQL = "DELETE FROM ${SUBSCRIPTIONS.name} WHERE ${ID.name} = ?"
private val getSubscriptionSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} WHERE ${ID.name} = ANY(?)"
private val getAllSubscriptionsSQL = "SELECT * FROM ${SUBSCRIPTIONS.name}"
