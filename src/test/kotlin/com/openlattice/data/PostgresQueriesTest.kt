package com.openlattice.data

import com.openlattice.data.storage.selectEntitySetWithCurrentVersionOfPropertyTypes
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class PostgresQueriesTest {
    private val logger: Logger = LoggerFactory.getLogger(PostgresQueriesTest::class.java)

    @Test
    fun testEmptySelectEntitySetWithCurrentVersionOfPropertyTypes() {
        logger.info(selectEntitySetWithCurrentVersionOfPropertyTypes(mapOf(), mapOf(), listOf(), mapOf(), mapOf(), setOf(), false, mapOf()))
    }
}