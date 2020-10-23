package com.openlattice.transporter

import com.google.common.base.Strings
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.TestServer
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.edm.type.PropertyType
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.IndexType
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTableManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.transporter.processors.TransporterSynchronizeTableDefinitionEntryProcessor
import com.openlattice.transporter.types.TransporterColumn
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import com.zaxxer.hikari.HikariDataSource
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.BeforeClass
import org.junit.Test
import org.postgresql.util.PSQLException
import java.util.*

fun pretty(query: String, e: PSQLException): String {
    val errorMessage = e.serverErrorMessage
    val buffer = if ( errorMessage == null ) {
        ""
    } else {
        Strings.repeat(" ", errorMessage.position-1) + "^"
    }
    return "Query failed:\n$query\n$buffer\n${e.message}"
}

class TransporterQueriesTest {
    companion object {
        lateinit var data: TransporterDatastore
        lateinit var transporter: HikariDataSource
        @BeforeClass
        @JvmStatic
        fun init() {
            val context = TestServer.testServer.context
            val configurationLoader = context.getBean(ConfigurationLoader::class.java)
            val config = configurationLoader.load(AssemblerConfiguration::class.java)
            val rhizome = context.getBean(RhizomeConfiguration::class.java)
            context.getBean(PostgresTableManager::class.java)
            val extDbConMan = context.getBean(ExternalDatabaseConnectionManager::class.java)

            data = TransporterDatastore(config, rhizome, extDbConMan)
            transporter = data.datastore()
            val fdwTables = transporter.connection.use { conn ->
                conn.createStatement()
                        .executeQuery("select count(*) from information_schema.foreign_tables where foreign_table_schema = '${TransporterDatastore.ENTERPRISE_FDW_SCHEMA}'").use { rs ->
                            rs.next()
                            rs.getInt(1)
                        }
            }
            println("$fdwTables foreign tables")
            context.getBeansOfType(TransporterDependent::class.java).forEach { (_, ss) ->
                println("initializing data for ${ss::class.java}")
                ss.init(data)
            }
        }
    }


    private val testTables = mutableSetOf<String>()

    private fun sync(type: UUID, props: Collection<PropertyType>) {
        val mockMap = mutableMapOf(type to TransporterColumnSet(emptyMap()))
        val ep = TransporterSynchronizeTableDefinitionEntryProcessor(props).init(data)
        ep.process(mockMap.entries.first())
        testTables.add(tableName(type))
    }

    @After
    fun cleanup() {
        transporter.connection.use { conn ->
            conn.createStatement().use {st ->
                val it = testTables.iterator()
                while (it.hasNext()) {
                    st.executeUpdate("DROP TABLE ${it.next()}")
                    it.remove()
                }
            }
        }
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
    fun testUpdateIdsForEntitySets() {
        val prop = TestDataFactory.propertyType(IndexType.NONE, false)
        val entityType = TestDataFactory.entityType(prop)
        val es = TestDataFactory.entitySetWithType(entityType.id)
        val entitySetId = es.id
        val table = tableName(entityType.id)
        sync(entityType.id, listOf(prop))
        val query = updatePrimaryKeyForEntitySets(table)
        transporter.connection.use {conn ->
            conn.prepareStatement(query).use {ps ->
                ps.setArray(1, PostgresArrays.createIntArray(conn, listOf(1)))
                ps.setArray(2, PostgresArrays.createUuidArray(conn, listOf(entitySetId)))
                try {
                    ps.executeQuery()
                } catch (e: PSQLException) {
                    fail(pretty(query, e))
                }
            }
        }
    }

    @Test
    fun testUpdateQuery() {
        val prop = TestDataFactory.propertyType(IndexType.NONE, false)
        val entityType = TestDataFactory.entityType(prop)
        val es = TestDataFactory.entitySetWithType(entityType.id)
        val entitySetId = es.id
        val table = tableName(entityType.id)
        sync(entityType.id, listOf(prop))
        val propCol = TransporterColumn(prop)
        val query = updateRowsForPropertyType(table, prop.id, propCol)
        transporter.connection.use {conn->
            conn.prepareStatement(query).use {ps ->
                ps.setArray(1, PostgresArrays.createIntArray(conn, listOf(1)))
                ps.setArray(2, PostgresArrays.createUuidArray(conn, entitySetId))
                ps.setArray(3, PostgresArrays.createUuidArray(conn, entitySetId))
                try {
                    ps.executeUpdate()
                } catch (e: PSQLException) {
                    fail(pretty(query, e))
                } catch (e: Exception) {
                    throw e
                }
            }
        }
    }
}