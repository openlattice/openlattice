package com.openlattice.transporter

import com.google.common.base.Strings
import com.openlattice.TestServer
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.mapstores.PropertyTypeMapstore
import com.openlattice.transporter.processors.TransporterSynchronizeTableDefinitionEntryProcessor
import com.openlattice.transporter.types.TransporterColumn
import com.openlattice.transporter.types.TransporterColumnSet
import com.zaxxer.hikari.HikariDataSource
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import org.mockito.Mockito
import org.postgresql.util.PSQLException
import java.util.*

fun transporter(): HikariDataSource {
    val assemblerConfiguration = TestServer.testServer.context.getBean(AssemblerConfiguration::class.java)
    val props = assemblerConfiguration.server.clone() as Properties
    return AssemblerConnectionManager.createDataSource("transporter", props, assemblerConfiguration.ssl)
}

fun pretty(query: String, e: PSQLException): String {
    val buffer = Strings.repeat(" ", e.serverErrorMessage.position-1) + "^"
    return "Query failed:\n$query\n$buffer\n${e.message}"
}

class TransporterQueriesTest {
    companion object {
        val transporter = transporter()
    }

    private fun sync(type: UUID, props: Collection<PropertyType>) {

        val acm = Mockito.mock(AssemblerConnectionManager::class.java)
        Mockito.`when`(acm.connect("transporter")).thenReturn(transporter)
        val mockMap = mutableMapOf(type to TransporterColumnSet(emptyMap()))
        val ep = TransporterSynchronizeTableDefinitionEntryProcessor(props)
        ep.init(acm)
        ep.process(mockMap.entries.first())
    }
    @Test
    fun testSynchronizeTableDefinition() {
        val type = UUID.randomUUID()
        val prop = TestDataFactory.propertyType(IndexType.NONE, false)
        val props = listOf(prop)
        sync(type, props)

        val cols = transporter.connection.use {conn ->
            conn.createStatement().use {st ->
                st.executeQuery("select * from \"et_$type\"").use { rs ->
                    (1..rs.metaData.columnCount).map { i -> rs.metaData.getColumnName(i) }.toSet()
                }
            }
        }
        assertEquals(setOf("id","entity_set_id","origin_id",prop.id.toString()), cols)
    }

    @Test
    fun updateIds() {
        val props = PropertyTypeMapstore(TestServer.hds)
        val entityTypeId = UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab")
        val entitySetId = UUID.fromString("4c38fd26-c616-4f8b-bfb6-b8581859effb")
        val propertyTypeId = UUID.fromString("1e6ff0f0-0545-4368-b878-677823459e57")
        val prop = props.load(propertyTypeId)
        val table = tableName(entityTypeId)
        sync(entityTypeId, listOf(prop))
        val query = updateIdsForEntitySets(table)
        transporter.connection.use {conn ->
            conn.prepareStatement(query).use {ps ->
                ps.setArray(1, PostgresArrays.createIntArray(conn, (1..257).toList()))
                ps.setArray(2, PostgresArrays.createUuidArray(conn, listOf(entitySetId)))
                try {
                    ps.executeUpdate()
                } catch (e: PSQLException) {
                    fail(pretty(query, e))
                }
            }
        }
    }

    @Test
    fun testUpdateQuery() {
        val props = PropertyTypeMapstore(TestServer.hds)
        val entityTypeId = UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab")
        val propertyTypeId = UUID.fromString("1e6ff0f0-0545-4368-b878-677823459e57")
        val entitySetId = UUID.fromString("4c38fd26-c616-4f8b-bfb6-b8581859effb")
        val prop = props.load(propertyTypeId)
        val table = tableName(entityTypeId)
        sync(entityTypeId, listOf(prop))
        val propCol = TransporterColumn(prop)
        val expected = TestServer.hds.connection.use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("select count(*) from data " +
                        "where entity_set_id = '${entitySetId}' " +
                        " and property_type_id = '${propertyTypeId}'").use {rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }
        val query = updateOneBatchForProperty(table, propertyTypeId, propCol)
        val updateCount = transporter.connection.use {conn->
            conn.prepareStatement(query).use {ps ->
                ps.setArray(1, PostgresArrays.createIntArray(conn, (0..257).toList()))
                ps.setArray(2, PostgresArrays.createUuidArray(conn, entitySetId))
                try {
                    ps.executeUpdate()
                } catch (e: PSQLException) {
                    fail(pretty(query, e))
                } catch (e: Exception) {
                    throw e
                }
            }
        }
        assertEquals("Expected update count", expected, updateCount)

        println("Success!")
    }
}