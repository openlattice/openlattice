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

/**
 * column bindings are
 * 1 - partitions array
 * 2 - entity set ids array
 */
private val checkQuery = "SELECT 1 " +
        "WHERE EXISTS (" +
        " SELECT 1 FROM ${PostgresTable.IDS.name} " +
        " WHERE ${PostgresColumn.PARTITION.name} = ANY(?) " +
        "  AND ${PostgresColumn.ENTITY_SET_ID.name} = ANY(?) " +
        "  AND ${DataTables.LAST_WRITE.name} > ${PostgresColumn.LAST_PROPAGATE.name}" +
        ")"

fun hasModifiedData(enterprise: HikariDataSource, partitions: Collection<Int>, entitySetIds: Set<UUID>): Boolean {
    return enterprise.connection.use { conn ->
        conn.prepareStatement(checkQuery).use { st ->
            val partitionsArray = PostgresArrays.createIntArray(conn, partitions)
            val entitySetArray = PostgresArrays.createUuidArray(conn, entitySetIds)
            st.setArray(1, partitionsArray)
            st.setArray(2, entitySetArray)
            val rs = st.executeQuery()
            rs.next()
        }
    }
}

val pkCols = listOf(PostgresColumn.ENTITY_SET_ID, PostgresColumn.ID)
val pk = pkCols.joinToString(", ") { it.name }

/**
 * column bindings are
 * 1 - partitions array
 * 2 - entity set ids array
 *
 */
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

    // this is almost certainly not necessary but if ids is already synced in the same transaction
    val createMissingRows = "INSERT INTO $destTable " +
            "($pk) " +
            "SELECT $pk " +
            "FROM src " +
            "WHERE ${PostgresColumn.VERSION.name} > 0 " +
            "ON CONFLICT ($pk) DO NOTHING"
    val modifyDestination = "UPDATE $destTable t " +
            "SET $destCol = src.$destCol " +
            "FROM src " +
            "WHERE " + pkCols.joinToString(" AND ") { "t." + it.name + " = src." + it.name}

    return "WITH src as (${updateLastPropagate}), inserted as ($createMissingRows) $modifyDestination"
}

/**
 * column bindings are
 * 1 - partitions array
 * 2 - entity set ids array
 *
 */
fun updateIdsForEntitySets(destTable: String): String {
    val updateLastPropagate = "UPDATE ${PostgresTable.IDS.name} " +
            "SET ${PostgresColumn.LAST_PROPAGATE.name} = ${DataTables.LAST_WRITE.name} " +
            "WHERE ${PostgresColumn.PARTITION.name} = ANY(?) " +
            " AND ${PostgresColumn.ENTITY_SET_ID.name} = ANY(?) " +
            " AND ${DataTables.LAST_WRITE.name} > ${PostgresColumn.LAST_PROPAGATE.name} " +
            "RETURNING ${PostgresColumn.ENTITY_SET_ID.name}, ${PostgresColumn.ID.name}, ${PostgresColumn.VERSION.name}"
    val createMissingRows = "INSERT INTO $destTable ($pk) " +
            "SELECT $pk " +
            "FROM src " +
            "WHERE ${PostgresColumn.VERSION.name} > 0 " +
            "ON CONFLICT ($pk) DO NOTHING"
    val deleteRows = "DELETE FROM $destTable t " +
            "USING src " +
            "WHERE src.${PostgresColumn.VERSION.name} <= 0 AND " +
            " " + pkCols.joinToString(" AND " ) { "t." + it.name + " = src." + it.name }
    return "WITH src as ($updateLastPropagate), " +
            "inserts as ($createMissingRows) " +
            deleteRows
}