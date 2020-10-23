package com.openlattice.search

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Principals
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.PERSISTENT_SEARCHES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.search.requests.PersistentSearch
import com.openlattice.search.requests.SearchConstraints
import com.zaxxer.hikari.HikariDataSource
import java.sql.Array
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement
import java.time.OffsetDateTime
import java.util.*

class PersistentSearchService(private val hds: HikariDataSource, private val spm: SecurePrincipalsManager) {

    private val mapper: ObjectMapper = ObjectMappers.getJsonMapper()

    private fun getUserAclKey(): AclKey {
        return spm.lookup(Principals.getCurrentUser())
    }

    private fun getUserAclKeyArray(connection: Connection): Array {
        return PostgresArrays.createUuidArray(connection, getUserAclKey())
    }

    fun createPersistentSearch(search: PersistentSearch): UUID {
        val connection = hds.connection
        connection.use {
            val ps: PreparedStatement = connection.prepareStatement(insertSql(connection, search))
            ps.setString(1, mapper.writeValueAsString(search.searchConstraints))
            ps.setString(2, mapper.writeValueAsString(search.alertMetadata))
            ps.setArray(3, PostgresArrays.createTextArray(connection, search.additionalEmailAddresses))
            ps.execute()
        }
        return search.id
    }

    fun loadPersistentSearchesForUser(includeExpired: Boolean): Iterable<PersistentSearch> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, loadPersistentSearchesSql(includeExpired)) { ps ->
            ps.setArray(1, getUserAclKeyArray(ps.connection))
        }) {
            ResultSetAdapters.persistentSearch(it)
        }
    }

    fun updatePersistentSearchLastRead(id: UUID, lastRead: OffsetDateTime) {
        val connection = hds.connection
        connection.use {
            val stmt: Statement = connection.createStatement()
            stmt.execute(updateDateSql(connection, id, false, lastRead))
        }
    }

    fun updatePersistentSearchAdditionalEmails(id: UUID, additionalEmails: Set<String>) {
        val connection = hds.connection
        connection.use {
            val ps: PreparedStatement = connection.prepareStatement(updateAdditionalEmailsSql(connection, id))
            ps.setArray(1, PostgresArrays.createTextArray(it, additionalEmails))
            ps.execute()
        }
    }

    fun updatePersistentSearchExpiration(id: UUID, expiration: OffsetDateTime) {
        val connection = hds.connection
        connection.use {
            val stmt: Statement = connection.createStatement()
            stmt.execute(updateDateSql(connection, id, true, expiration))
        }
    }

    fun updatePersistentSearchConstraints(id: UUID, constraints: SearchConstraints) {
        val connection = hds.connection
        connection.use {
            val ps: PreparedStatement = connection.prepareStatement(updateSearchConstraintsSql(connection, id))
            ps.setString(1, mapper.writeValueAsString(constraints))
            ps.execute()
        }
    }

    private fun insertSql(connection: Connection, search: PersistentSearch): String {
        val columns = setOf(
                ID_VALUE.name,
                ACL_KEY.name,
                LAST_READ.name,
                EXPIRATION_DATE.name,
                ALERT_TYPE.name,
                SEARCH_CONSTRAINTS.name,
                ALERT_METADATA.name,
                EMAILS.name
        )

        return "INSERT INTO ${PERSISTENT_SEARCHES.name} ( ${columns.joinToString()} ) " +
                "VALUES ('${search.id}'::uuid, '${getUserAclKeyArray(connection)}', '${search.lastRead}', '${search.expiration}', " +
                "'${search.type}', ?::jsonb, ?::jsonb, ?)"
    }

    private fun updateAdditionalEmailsSql(connection: Connection, id: UUID): String {
        return "UPDATE ${PERSISTENT_SEARCHES.name} SET ${EMAILS.name} = ? " +
                "WHERE id = '$id' AND ${ACL_KEY.name} = '${getUserAclKeyArray(connection)}'"
    }

    private fun updateDateSql(connection: Connection, id: UUID, isExpiration: Boolean, value: OffsetDateTime): String {
        val field = if (isExpiration) EXPIRATION_DATE.name else LAST_READ.name
        return "UPDATE ${PERSISTENT_SEARCHES.name} SET $field = '$value' " +
                "WHERE id = '$id' AND ${ACL_KEY.name} = '${getUserAclKeyArray(connection)}'"
    }

    private fun updateSearchConstraintsSql(connection: Connection, id: UUID): String {
        return "UPDATE ${PERSISTENT_SEARCHES.name} SET ${SEARCH_CONSTRAINTS.name} = ?::jsonb " +
                "WHERE id = '$id' AND ${ACL_KEY.name} = '${getUserAclKeyArray(connection)}'"
    }

    private fun loadPersistentSearchesSql(includeExpired: Boolean): String {
        var sql = "SELECT * FROM ${PERSISTENT_SEARCHES.name} WHERE ${ACL_KEY.name} = ?"
        if (!includeExpired) {
            sql += " AND ${EXPIRATION_DATE.name} > now()"
        }
        return sql
    }
}