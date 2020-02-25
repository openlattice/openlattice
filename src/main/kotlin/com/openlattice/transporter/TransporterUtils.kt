package com.openlattice.transporter

import com.openlattice.ApiUtil
import com.openlattice.IdConstants
import com.openlattice.postgres.*
import com.openlattice.transporter.types.TransporterColumn
import com.zaxxer.hikari.HikariDataSource
import java.util.*

fun tableName(entityTypeId: UUID): String {
    return ApiUtil.dbQuote("et_$entityTypeId")
}

fun tableDefinition(entityTypeId: UUID, columns: Collection<PostgresColumnDefinition>): PostgresTableDefinition {
    val definition = PostgresTableDefinition(tableName(entityTypeId))
    definition.addColumns(*columns.toTypedArray())
    return definition
}

private val checkQuery = "SELECT 1 " +
        "WHERE EXISTS (" +
        " SELECT 1 FROM ${PostgresTable.DATA.name} " +
        " WHERE ${PostgresColumn.ENTITY_SET_ID.name} = ANY(?) " +
        "  AND ${PostgresColumn.PARTITION.name} = ANY(?) " +
        "  AND ${PostgresColumn.PROPERTY_TYPE_ID.name} = ANY(?) " +
        "  AND ${DataTables.LAST_WRITE.name} > ${PostgresColumn.LAST_PROPAGATE.name}" +
        ")"

fun hasModifiedData(enterprise: HikariDataSource, entitySetIds: Set<UUID>, properties: Collection<UUID>, partitions: Collection<Int>): Boolean {
    return enterprise.connection.use { conn ->
        conn.prepareStatement(checkQuery).use { st ->
            val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
            val partitionsArray = PostgresArrays.createIntArray(conn, partitions)
            val propsArray = PostgresArrays.createUuidArray(conn, properties)
            st.setArray(1, entitySetArray)
            st.setArray(2, partitionsArray)
            st.setArray(3, propsArray)
            val rs = st.executeQuery()
            rs.next()
        }
    }
}

fun updateOneBatchForProperty(destTable: String, propCol: Map.Entry<UUID, TransporterColumn>): String {
    val srcCol = propCol.value.srcCol
    val destCol = propCol.value.destColName
    val id = propCol.key

    val updateLastPropagate = "UPDATE ${PostgresTable.DATA.name} " +
            "SET ${PostgresColumn.LAST_PROPAGATE.name} = ${DataTables.LAST_WRITE.name} " +
            "WHERE ${PostgresColumn.PARTITION.name} = ANY(?) " +
            " AND ${PostgresColumn.ENTITY_SET_ID.name} = ANY(?) " +
            " AND ${PostgresColumn.PROPERTY_TYPE_ID.name} = '${id}' " +
            " AND ${PostgresColumn.ORIGIN_ID.name} = '${IdConstants.EMPTY_ORIGIN_ID.id}' " +
            " AND ${DataTables.LAST_WRITE.name} > ${PostgresColumn.LAST_PROPAGATE.name} " +
            "RETURNING ${PostgresColumn.ENTITY_SET_ID.name}, ${PostgresColumn.ID.name}, ${PostgresColumn.VERSION.name}, " +
            " CASE WHEN ${PostgresColumn.VERSION.name} > 0 then $srcCol else null end as $destCol"

    val pk = listOf(PostgresColumn.ENTITY_SET_ID, PostgresColumn.ID).joinToString(", ") { it.name }
    val cols = "$pk,$destCol"

    val modifyDestination = "INSERT INTO $destTable " +
            "($cols) " +
            "SELECT $cols " +
            "FROM src " +
            "WHERE ${PostgresColumn.VERSION.name} != 0 " +
            "ON CONFLICT ($pk) DO UPDATE " +
            "SET $destCol = excluded.$destCol"

    return "WITH src as (${updateLastPropagate}) $modifyDestination"
}
