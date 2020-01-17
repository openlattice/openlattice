package com.openlattice.shuttle.payload

import com.google.common.util.concurrent.RateLimiter
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.SQLException

private val logger = LoggerFactory.getLogger(JdbcPayload::class.java)
const val DEFAULT_PERMITS_PER_SECOND = 10_000.0
internal const val DEFAULT_FETCH_SIZE = 50000

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class JdbcPayload @JvmOverloads constructor(
        permitsPerSecond: Double = DEFAULT_PERMITS_PER_SECOND,
        val hds: HikariDataSource,
        val sql: String,
        val fetchSize: Int = DEFAULT_FETCH_SIZE,
        val rateLimited: Boolean = true
) : Payload {
    private val rateLimiter = RateLimiter.create(if (rateLimited) permitsPerSecond else Double.MAX_VALUE)
    private lateinit var columns: List<String>

    override fun getPayload(): BasePostgresIterable<Map<String, Any?>> {
        return BasePostgresIterable(StatementHolderSupplier(hds, sql, fetchSize)) { rs ->
            if (!::columns.isInitialized) {
                columns = getColumnNames(rs)
            }

            if (rateLimited) rateLimiter.acquire()

            read(columns, rs)
        }

    }
}

private fun getColumnNames(rs: ResultSet): List<String> {
    try {
        val rsm = rs.getMetaData()
        val columnCount = rsm!!.columnCount
        return (1..columnCount).map(rsm::getColumnName)
    } catch (e: SQLException) {
        throw IllegalStateException("ResultSummary Set Iterator initialization failed")
    }
}

private fun read(columns: List<String>, rs: ResultSet): Map<String, Any?> {
    return columns.associateWith { col ->
        try {
            when (val obj: Any? = rs.getObject(col)) {
                is ByteArray -> obj
                else -> obj?.toString()
            }

        } catch (e: SQLException) {
            logger.error("Unable to read col {}.", col, e)
        }
    }
}