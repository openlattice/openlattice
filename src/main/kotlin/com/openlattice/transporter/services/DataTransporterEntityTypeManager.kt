package com.openlattice.transporter.services

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.openlattice.IdConstants
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PostgresEdmTypeConverter
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.util.*
import java.util.concurrent.ConcurrentHashMap

const val batchSize = 64_000
const val olSchema = "ol"
const val transporterSchema = "transporter"

private fun PostgresTableDefinition.defaults(): PostgresTableDefinition {
    this.addColumns(
            ENTITY_SET_ID,
            ID_VALUE
    )
    this.primaryKey(
            ENTITY_SET_ID,
            ID_VALUE
    )
    return this
}

private fun quote(name: String): String {
    return "\"$name\""
}

class DataTransporterEntityTypeManager(
        private val et: EntityType,
        private val dataModelService: EdmManager,
        private val partitionManager: PartitionManager,
        private val entitySetService: EntitySetManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(DataTransporterEntityTypeManager::class.java)
        private val loader = object : CacheLoader<EdmPrimitiveTypeKind, Optional<PostgresDatatype>>() {
            override fun load(key: EdmPrimitiveTypeKind): Optional<PostgresDatatype> {
                return try {
                    val single = PostgresEdmTypeConverter.map(key)
                    Optional.of(single)
                } catch (e: java.lang.UnsupportedOperationException) {
                    logger.warn("Unsupported EDM data type {}", key)
                    Optional.empty()
                }
            }
        }
        private val datatypes = CacheBuilder.newBuilder().build(loader)
    }
    private val table = PostgresTableDefinition(quote("et_${et.id}")).defaults()

    private val validProperties = dataModelService.getPropertyTypes(et.properties)
                .asSequence()
                .map {prop ->
                    column(prop).map { col ->
                        table.addColumns(col)
                        prop.id to prop
                    }
                }
                .filter { it.isPresent }
                .map { it.get() }
                .toMap(ConcurrentHashMap<UUID, PropertyType>())

    private fun executeLog(st: Statement, sql: String): Boolean {
        return try {
            logger.debug(sql)
            st.execute(sql)
        } catch (e: SQLException) {
            logger.error("Error running $sql", e)
            throw e
        }
    }

    private fun column(prop: PropertyType): Optional<PostgresColumnDefinition> {
        val type = datatypes.get(prop.datatype)
        return type.map {type ->
            val col = quote(prop.id.toString())
            PostgresColumnDefinition(col, type)
        }
    }

    fun addPropertyTypes(transporter: HikariDataSource, propertyTypes: Collection<PropertyType>) {
        transporter.connection.use {conn ->
            conn.autoCommit = false
            val newValidProps = propertyTypes.map {pt ->
                column(pt).map { col -> pt to col }
            }
                    .filter { it.isPresent }
                    .map { it.get() }

            this.synchronizeColumns(conn, newValidProps.map { it.second })

            conn.commit()
            newValidProps.forEach {(pt, col) ->
                if (validProperties.putIfAbsent(pt.id, pt) == null) {
                    table.addColumns(col)
                }
            }
        }
    }

    fun createTable(transporter: HikariDataSource, enterprise: HikariDataSource) {
        transporter.connection.use { c ->
            c.autoCommit = false
            c.createStatement().use { st ->
                executeLog(st, table.createTableQuery())
            }
            this.synchronizeColumns(c, table.columns.filter { it.name.startsWith("\"") })
            c.commit()
        }
        // importTableIntoEnterprise(enterprise)
    }

    private fun synchronizeColumns(conn: Connection, cols: Collection<PostgresColumnDefinition>) {
        val rsmd = conn.createStatement().executeQuery("SELECT * FROM ${table.name} where 1 = 0").metaData
        val currentCols = (1..rsmd.columnCount)
                .map {i -> quote(rsmd.getColumnName(i)) }
                .toSet()
        val colMap = cols.associateBy { it.name }.toMutableMap()
        colMap.keys.removeAll(currentCols)
        if (colMap.isNotEmpty()) {
            logger.info("adding missing columns {} for entity type {} ({})", colMap.keys, et.type, et.id)
            conn.createStatement().use { st ->
                val query = "ALTER TABLE ${table.name} " + colMap.values.joinToString(", ") { col -> " ADD COLUMN ${col.name} ${col.datatype.sql()}"}
                logger.info(query)
                executeLog(st, query)
            }
        }

    }

    private fun importTableIntoEnterprise(enterprise: HikariDataSource) {
        enterprise.connection.use { c ->
            c.createStatement().use {st ->
                val drop = "DROP FOREIGN TABLE IF EXISTS transporter.${table.name}"
                val createForeignTable = "IMPORT FOREIGN SCHEMA public LIMIT TO ( ${table.name} ) FROM SERVER transporter INTO transporter;"
                executeLog(st, drop)
                executeLog(st, createForeignTable)
            }
        }
    }

    fun removeTable(transporter: HikariDataSource, enterprise: HikariDataSource) {
        transporter.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP TABLE IF EXISTS ${table.name}")
            }
        }
    }

    fun updateAllEntitySets(enterprise: HikariDataSource, transporter: HikariDataSource) {
        val entitySets = entitySetService.getEntitySetsOfType(this.et.id)
                .filter { !it.isLinking && !it.flags.contains(EntitySetFlag.AUDIT) }
                .toSet()
        updateEntitySets(enterprise, transporter, entitySets)
    }

    fun updateEntitySets(enterprise: HikariDataSource, transporter: HikariDataSource, sets: Set<EntitySet>) {
        sets.forEach {
            if (it.isLinking) {
                throw IllegalArgumentException("entity set ${it.name} (${it.id}) is a linking entity set")
            }
        }
        val entitySetIds = entitySetIds(sets)

        if (!hasModifiedData(enterprise, entitySetIds)) {
            logger.info("No modified rows in entity type {}", et.type)
            return
        }
        transporter.connection.use { conn ->
            conn.autoCommit = false
            var lastSql = ""
            try {
                val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
                val partitions = PostgresArrays.createIntArray(conn, partitions(entitySetIds))
                val updates = this.validProperties.values.map { pt ->
                    val query = updateOneBatchForProperty(pt)
                    lastSql = query
                    conn.prepareStatement(query).use { ps ->
                        ps.setArray(1, partitions)
                        ps.setArray(2, entitySetArray)
                        generateSequence { ps.executeUpdate() }.takeWhile { it > 0 }.sum()
                    }
                }.sum()
                logger.info("Updated {} values in entity type {}", updates, et.type)
                conn.commit()
            } catch (ex: Exception) {
                logger.error("Unable to update transporter: SQL: {}", lastSql, ex)
                conn.rollback()
                throw ex
            }
        }
    }
    private fun getSourceColumnName(propertyType: PropertyType): String {
        return PostgresDataTables.getSourceDataColumnName(
            datatypes[propertyType.datatype].get(),
                propertyType.postgresIndexType)
    }

    private fun entitySetIds(sets: Set<EntitySet>): Set<UUID> {
        return sets.associateBy { it.id }.keys
    }

    private fun partitions(entitySetIds: Set<UUID>): Collection<Int> {
        return partitionManager.getPartitionsByEntitySetId(entitySetIds).values.flatten().toSet()
    }

    private fun updateOneBatchForProperty(propertyType: PropertyType): String {
        val srcCol = getSourceColumnName(propertyType)
        val destCol = quote(propertyType.id.toString())

        val updateLastPropagate = "UPDATE ${DATA.name} " +
                "SET ${LAST_PROPAGATE.name} = ${LAST_WRITE.name} " +
                "WHERE ${PARTITION.name} = ANY(?) " +
                " AND ${ENTITY_SET_ID.name} = ANY(?) " +
                " AND ${PROPERTY_TYPE_ID.name} = '${propertyType.id}' " +
                " AND ${ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
                " AND ${LAST_WRITE.name} > ${LAST_PROPAGATE.name} " +
                "RETURNING ${ENTITY_SET_ID.name}, ${ID.name}, ${VERSION.name}, " +
                " CASE WHEN ${VERSION.name} > 0 then $srcCol else null end as $destCol"

        val pk = listOf(ENTITY_SET_ID, ID).joinToString(", ") { it.name }
        val cols = "$pk,$destCol"

        val modifyDestination = "INSERT INTO ${table.name} " +
                "($cols) " +
                "SELECT $cols " +
                "FROM src " +
                "WHERE ${VERSION.name} != 0 " +
                "ON CONFLICT ($pk) DO UPDATE " +
                "SET $destCol = excluded.$destCol"

        return "WITH src as (${updateLastPropagate}) $modifyDestination"
    }

    private val checkQuery = "SELECT 1 " +
            "WHERE EXISTS (" +
            " SELECT 1 FROM ${DATA.name} " +
            " WHERE ${ENTITY_SET_ID.name} = ANY(?) " +
            "  AND ${PARTITION.name} = ANY(?) " +
            "  AND ${PROPERTY_TYPE_ID.name} = ANY(?) " +
            "  AND ${LAST_WRITE.name} > ${LAST_PROPAGATE.name}" +
            ")"

    private fun hasModifiedData(enterprise: HikariDataSource, entitySetIds: Set<UUID>): Boolean {
        return enterprise.connection.use { conn ->
            conn.prepareStatement(checkQuery).use { st ->
                val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
                val partitionsArray = PostgresArrays.createIntArray(conn, partitions(entitySetIds))
                val propsArray = PostgresArrays.createUuidArray(conn, validProperties.keys)
                st.setArray(1, entitySetArray)
                st.setArray(2, partitionsArray)
                st.setArray(3, propsArray)
                val rs = st.executeQuery()
                rs.next()
            }
        }
    }
}