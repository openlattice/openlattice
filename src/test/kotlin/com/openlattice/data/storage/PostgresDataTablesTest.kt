package com.openlattice.data.storage

import com.openlattice.postgres.PostgresDataTables
import org.junit.Test
import org.slf4j.LoggerFactory

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDataTablesTest {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresDataTablesTest::class.java)
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