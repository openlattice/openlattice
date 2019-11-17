package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.USERS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource

const val DELETE_BATCH_SIZE = 1024
private val markUserSql = "UPDATE ${USERS.name} SET ${LAST_MARK.name} = now() WHERE ${USER_ID.name} = ?"
private val expiredUsersSql = "SELECT ${USER_ID.name} from ${USERS.name} WHERE ${EXPIRATION.name} < ? "

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Auth0SyncService(val hds: HikariDataSource ) {
    private val mapper = ObjectMappers.newJsonMapper()
    fun markUser(user: User) {
        markUser(user.id)
    }

    fun markUser(userId: String) {
        hds.connection.use { connection ->
            connection.prepareStatement(markUserSql).use { ps ->
                ps.setString(1, userId)
                ps.executeUpdate()
            }
        }
    }

    fun getExpiredUsers(): BasePostgresIterable<User> {
        val expirationThreshold = System.currentTimeMillis() - 6 * REFRESH_INTERVAL_MILLIS
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, expiredUsersSql, DELETE_BATCH_SIZE) { ps ->
                    ps.setLong(1, expirationThreshold)
                }) { rs -> mapper.readValue<User>(rs.getString(USER.name)) }
    }
}


