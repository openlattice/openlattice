package com.openlattice.ids


import com.google.common.base.Preconditions.checkNotNull
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.*

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IdCatchupEntryProcessor(hds: HikariDataSource) : AbstractRhizomeEntryProcessor<Int, Range, Void>() {

    private val hds: HikariDataSource = checkNotNull(hds)

    companion object {
        private val logger = LoggerFactory.getLogger(IdCatchupEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<Int, Range>): Void? {
        val range = checkNotNull(entry.value) //Range should never be null in the EP.
        var counter = 0
        try {
            hds.connection.use { connection ->
                prepareExistQuery(connection).use { ps ->

                    while (exists(ps, range.peek())) {
                        range.nextId()
                        counter++
                        if ((counter % 10000) == 0) {
                            logger.info("Incremented range with base {} by {}", range.base, counter)
                        }
                    }

                    entry.setValue(range)
                    if (counter > 0) {
                        logger.warn("Caught up range with base {} by {} increments", range.base, counter)
                    }
                }
            }
        } catch (e: SQLException) {
            logger.error("Error catching up ranges.", e)
        }

        return null
    }

    @Throws(SQLException::class)
    fun prepareExistQuery(connection: Connection): PreparedStatement {
        return connection.prepareStatement("SELECT count(*) from ${PostgresTable.IDS.name} WHERE ${PostgresColumn.ID_VALUE.name} = ?")
    }

    @Throws(SQLException::class)
    fun exists(ps: PreparedStatement, id: UUID): Boolean {
        ps.setObject(1, id)
        //Count query always guaranteed to have one row.
        val rs = ps.executeQuery()
        rs.next()
        return ResultSetAdapters.count(rs) > 0
    }
}

