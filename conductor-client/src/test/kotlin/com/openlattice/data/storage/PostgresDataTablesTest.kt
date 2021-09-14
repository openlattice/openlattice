package com.openlattice.data.storage

import com.openlattice.postgres.PostgresDataTables
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDataTablesTest {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresDataTablesTest::class.java)
    }

    @Test
    fun testIds() {
        logger.info("(0,0) = ${UUID(0, 0)}")
        logger.info("(0,2) = ${UUID(0, 1)}")
        val id = "00000000-0000-0000-8000-00000000002c"
        logger.info("$id = ${UUID.fromString(id).leastSignificantBits.toInt()}")
    }

    @Test
    fun testQuery() {
        val tableDefinition = PostgresDataTables.buildDataTableDefinition()
        logger.info("create table sql: {}", tableDefinition.createTableQuery())
        tableDefinition.createIndexQueries.forEach { sql ->
            logger.info("create index sql: {}", sql)
        }
    }
}