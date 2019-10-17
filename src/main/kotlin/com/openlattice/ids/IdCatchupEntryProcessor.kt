package com.openlattice.ids


import com.google.common.base.Preconditions.checkNotNull
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.ResultSetAdapters.id
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IdCatchupEntryProcessor(hds: HikariDataSource) : AbstractRhizomeEntryProcessor<Int, Range?, Void>() {

    private val hds: HikariDataSource = checkNotNull(hds)

    companion object {

        private val logger = LoggerFactory.getLogger(IdCatchupEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<Int, Range?>): Void? {
        val range = entry.value!! //Range should never be null in the EP.
        val lowerbound = Range(range.base).nextId()
        val supremum = Range(range.base + (1 shl 48)).nextId()

        try {
            hds.connection.use { connection ->
                prepareLatestQuery(connection).use { ps ->
                    ps.setObject(1, lowerbound)
                    ps.setObject(2, supremum)
                    val rs = ps.executeQuery()
                    val newMaxRange = if (rs.next()) {
                        val id = id(rs)
                        val r = Range(
                                range.base,
                                id.mostSignificantBits xor range.base,
                                id.leastSignificantBits
                        )
                        r.nextId()
                        r
                    } else {
                        logger.warn("Detected empty range with base {} ({})", range.base, range.base ushr 48)
                        Range(range.base) //This is -2 so that increment takes it to -1
                    }
                    entry.setValue(newMaxRange)
                }
            }
        } catch (e: SQLException) {
            logger.error("Error catching up ranges.", e)
        }

        return null
    }

    /**
     * This only works because the base represents the 16 most significant bits in the UUID. This query is intended to
     * be used with the lowest possible UUID for a given base and the lowest possible UUID for the immediately larger
     * base.
     *
     * @param connection A jdbc connection to a database with a valid [PostgresTable.IDS] table.
     * @return A prepared statement that returns the largest UUID in a given base. As long as preceeding assumptions are
     * met.
     */
    @Throws(SQLException::class)
    private fun prepareLatestQuery(connection: Connection): PreparedStatement {
        return connection.prepareStatement(
                "SELECT ${PostgresColumn.ID.name} from ${PostgresTable.IDS.name} WHERE ${PostgresColumn.ID_VALUE.name} >= ? AND ${PostgresColumn.ID_VALUE.name} < ? ORDER BY ID DESC LIMIT 1"
        )
    }

}

