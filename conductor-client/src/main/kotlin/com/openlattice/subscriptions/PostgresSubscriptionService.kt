package com.openlattice.subscriptions

import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.SUBSCRIPTIONS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

class PostgresSubscriptionService(
        private val hds: HikariDataSource,
        private val mapper: ObjectMapper
) : SubscriptionService {
    // v code v
    override fun createOrUpdateSubscription(subscription: Subscription, user: Principal) {
        hds.connection.use { conn ->

            conn.prepareStatement(createOrUpdateSubscriptionSQL).use { ps ->
                val srcString = mapper.writeValueAsString(subscription.query.srcSelections)
                val dstString = mapper.writeValueAsString(subscription.query.dstSelections)
                val (entitySetId, id) = subscription.query.ids.entries.map { it.key to it.value.get().first() }.first()
                ps.setObject(1, user.id)
                ps.setObject(2, entitySetId)
                ps.setObject(3, id)
                ps.setString(4, srcString)
                ps.setString(5, dstString)
                ps.setString(6, mapper.writeValueAsString(subscription.contact))

                print(ps.toString())
                ps.executeUpdate()
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

    override fun getAllSubscriptions(): Iterable<Pair<Principal, Subscription>> {
        return execSqlSelectReturningIterable(getAllSubscriptionsUnfilteredSQL,
                                              { rs: ResultSet ->
                                                  Principal(
                                                          PrincipalType.USER,
                                                          rs.getString(PRINCIPAL_ID.name)
                                                  ) to ResultSetAdapters.subscriptionContact(rs)
                                              }, { ps: PreparedStatement, _: Connection -> ps })
    }

    override fun getAllSubscriptions(user: Principal): Iterable<Subscription> {
        return execSqlSelectReturningIterable(getAllSubscriptionsSQL,
                                              { rs: ResultSet -> ResultSetAdapters.subscriptionContact(rs) },
                                              { ps: PreparedStatement, _: Connection ->
                                                  ps.setObject(1, user.id)
                                                  print(ps.toString())
                                                  ps
                                              }
        )
    }

    override fun getSubscriptions(ekIds: List<UUID>, user: Principal): Iterable<Subscription> {
        return execSqlSelectReturningIterable(getSubscriptionSQL,
                                              { rs: ResultSet -> ResultSetAdapters.subscriptionContact(rs) },
                                              { ps: PreparedStatement, conn: Connection ->
                                                  val arr = PostgresArrays.createUuidArray(conn, ekIds)
                                                  ps.setObject(1, user.id)
                                                  ps.setArray(2, arr)
                                                  print(ps.toString())
                                                  ps
                                              }
        )
    }

// ^ code ^
// v util v

    private fun <T> execSqlSelectReturningIterable(
            sql: String,
            resultMappingFunc: (rs: ResultSet) -> T,
            statementFunction: (ps: PreparedStatement, conn: Connection) -> PreparedStatement
    ): Iterable<T> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, sql, 0, false) {
            statementFunction(it, it.connection)
        }) { rs -> resultMappingFunc(rs) }
    }

}

private val createOrUpdateSubscriptionSQL = "INSERT INTO ${SUBSCRIPTIONS.name} " +
        "(${PRINCIPAL_ID.name},${ENTITY_SET_ID.name}, ${ID.name}, ${SRC_SELECTS.name}, ${DST_SELECTS.name},${CONTACT_INFO.name},${LAST_NOTIFIED.name})" +
        " VALUES (?,?::uuid,?::uuid,?::jsonb,?::jsonb,?::jsonb, now())" +
        " ON CONFLICT ( ${PRINCIPAL_ID.name}, ${ID.name} ) DO UPDATE " +
        " SET ${ENTITY_SET_ID.name} = EXCLUDED.${ENTITY_SET_ID.name}, ${SRC_SELECTS.name} = EXCLUDED.${SRC_SELECTS.name}," +
        "  ${DST_SELECTS.name} = EXCLUDED.${DST_SELECTS.name}, ${ORGANIZATION_ID.name} = EXCLUDED.${ORGANIZATION_ID.name}," +
        "  ${CONTACT_INFO.name} = EXCLUDED.${CONTACT_INFO.name}, ${LAST_NOTIFIED.name} = EXCLUDED.${LAST_NOTIFIED.name}"

private val markLastNotifiedSQL = "UPDATE ${SUBSCRIPTIONS.name}" +
        " SET ${LAST_NOTIFIED.name} = now()" +
        " WHERE ${ID.name} = ? AND ${PRINCIPAL_ID.name} = ?"

private val deleteSubscriptionSQL = "DELETE FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ? AND ${ID.name} = ?"
private val getSubscriptionSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ? AND ${ID.name} = ANY(?)"
private val getAllSubscriptionsSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} WHERE ${PRINCIPAL_ID.name} = ?"
private val getAllSubscriptionsUnfilteredSQL = "SELECT * FROM ${SUBSCRIPTIONS.name} "
