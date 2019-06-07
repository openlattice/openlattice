package com.openlattice.subscriptions

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.graph.NeighborhoodQuery
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.SUBSCRIPTIONS
import com.openlattice.postgres.PostgresTable.SUBSCRIPTION_CONTACTS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.OffsetDateTime
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
                conn.prepareStatement(createOrUpdateSubscriptionSQL).use { ps ->
                    val srcString = mapper.writeValueAsString(subscription.srcSelections)
                    val dstString = mapper.writeValueAsString(subscription.dstSelections)
                    ps.setObject(1, user.id)
                    ps.setObject(2, ekid)
                    ps.setObject(3, srcString)
                    ps.setObject(4, dstString)
                    print(ps.toString())
                    ps.executeUpdate()
                }
            }
        }
    }

    override fun createOrUpdateSubscriptionContact(contactInfo: SubscriptionContact, user: Principal) {
        hds.connection.use { conn ->
            contactInfo.subscription.ids.map { id ->
                conn.prepareStatement(createOrUpdateSubscriptionContactSQL).use { ps ->
                    ps.setObject(1, user.id)
                    ps.setObject(2, id)
                    ps.setObject(3, mapper.writeValueAsString(contactInfo.contact))
                    print(ps.toString())
                    ps.executeUpdate()
                }
            }
        }
    }

    override fun markLastNotified(ekIds: Set<UUID>, user: Principal) {
        hds.connection.use { conn ->
            ekIds.forEach { id ->
                conn.prepareStatement(markLastNotifiedSQL).use { ps ->
                    ps.setObject(1, id)
                    ps.setObject(2, user.id)
                    ps.executeUpdate()
                }
            }
        }
    }

    override fun deleteSubscription(ekId: UUID, user: Principal) {
        hds.connection.use { conn ->
            val ps = conn.prepareStatement(deleteSubscriptionSQL)
            ps.setObject(1, user.id)
            ps.setObject(2, ekId)
            print(ps.toString())
            ps.executeUpdate()
        }
    }

    override fun getAllSubscriptions(): Iterable<Pair<Principal, SubscriptionContact>> {
        return execSqlSelectReturningIterable(getAllSubscriptionsUnfilteredSQL,
                                              { rs: ResultSet ->
                                                  Principal(
                                                          PrincipalType.USER,
                                                          rs.getString(PRINCIPAL_ID.name)
                                                  ) to SubscriptionContact(
                                                          ResultSetAdapters.subscription(rs),
                                                          mapper.readValue(
                                                                  rs.getString(CONTACT_INFO.name),
                                                                  object : TypeReference<Map<SubscriptionContactType, SubscriptionContact>>() {}
                                                          ),
                                                          rs.getObject(LAST_NOTIFIED_FIELD, OffsetDateTime::class.java)
                                                  )
                                              }, { ps: PreparedStatement, _: Connection -> ps })
    }

    override fun getAllSubscriptions(user: Principal): Iterable<NeighborhoodQuery> {
        return execSqlSelectReturningIterable(getAllSubscriptionsSQL,
                                              { rs: ResultSet -> ResultSetAdapters.subscription(rs) },
                                              { ps: PreparedStatement, _: Connection ->
                                                  ps.setObject(1, user.id)
                                                  print(ps.toString())
                                                  ps
                                              }
        )
    }

    override fun getSubscriptions(ekIds: List<UUID>, user: Principal): Iterable<NeighborhoodQuery> {
        return execSqlSelectReturningIterable(getSubscriptionSQL,
                                              { rs: ResultSet -> ResultSetAdapters.subscription(rs) },
                                              { ps: PreparedStatement, conn: Connection ->
                                                  val arr = PostgresArrays.createUuidArray(conn, ekIds)
                                                  ps.setObject(1, user.id)
                                                  ps.setObject(2, arr)
                                                  print(ps.toString())
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
    ): Iterable<T> {
        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    connection.autoCommit = false
                    val ps = statementFunction(connection.prepareStatement(sql), connection)
                    val rs = ps.executeQuery()
                    StatementHolder(connection, ps, rs)
                },
                Function<ResultSet, T> { rs -> resultMappingFunc(rs) }
        )
    }

}

private val createOrUpdateSubscriptionSQL = "INSERT INTO ${SUBSCRIPTIONS.name} " +
        "(${PRINCIPAL_ID.name}, ${ID.name}, ${SRC_SELECTS.name}, ${DST_SELECTS.name})" +
        " VALUES (?,?::uuid,?::jsonb,?::jsonb)" +
        " ON CONFLICT ( ${PRINCIPAL_ID.name}, ${ID.name} ) DO UPDATE " +
        " SET ${SRC_SELECTS.name} = EXCLUDED.${SRC_SELECTS.name}, ${DST_SELECTS.name} = EXCLUDED.${DST_SELECTS.name}"

private val createOrUpdateSubscriptionContactSQL = "INSERT INTO ${SUBSCRIPTION_CONTACTS.name} " +
        "(${PRINCIPAL_ID.name}, ${ID.name}, ${CONTACT_INFO.name})" +
        " VALUES (?,?::uuid,?)" +
        " ON CONFLICT ( ${PRINCIPAL_ID.name}, ${ID.name} ) DO UPDATE " +
        " SET ${CONTACT_INFO.name} = EXCLUDED.${CONTACT_INFO.name}"

private val markLastNotifiedSQL = "UPDATE ${SUBSCRIPTIONS.name}" +
        " SET ${LAST_NOTIFIED.name} = now()" +
        " WHERE ${ID.name} = ? AND ${PRINCIPAL_ID.name} = ?"

private val deleteSubscriptionSQL = "DELETE FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ? AND ${ID.name} = ?"
private val getSubscriptionSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ? AND ${ID.name} = ANY(?)"
private val getAllSubscriptionsSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ?"
private val getAllSubscriptionsUnfilteredSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} INNER JOIN ${SUBSCRIPTION_CONTACTS.name} USING( $ID.name )"
