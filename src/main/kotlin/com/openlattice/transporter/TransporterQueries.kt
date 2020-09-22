package com.openlattice.transporter

import com.openlattice.ApiUtil
import com.openlattice.IdConstants
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ID_VALUE
import com.openlattice.postgres.PostgresColumn.LAST_TRANSPORT
import com.openlattice.postgres.PostgresColumn.LINKING_ID
import com.openlattice.postgres.PostgresColumn.ORIGIN_ID
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresColumn.PROPERTY_TYPE_ID
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresExpressionIndexDefinition
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTableDefinition
import com.openlattice.transporter.types.TransporterColumn
import com.zaxxer.hikari.HikariDataSource
import java.util.*

const val transporterNamespace = "transporter_data"

private val transportTimestampColumn: PostgresColumnDefinition = LAST_TRANSPORT

fun unquotedTableName(entityTypeId: UUID): String {
    return "et_$entityTypeId"
}

fun tableName(entityTypeId: UUID): String {
    return ApiUtil.dbQuote(unquotedTableName(entityTypeId))
}

fun tableDefinition(entityTypeId: UUID, propertyColumns: Collection<PostgresColumnDefinition>): PostgresTableDefinition {
    val definition = PostgresTableDefinition(tableName(entityTypeId))
    val indexPrefix = unquotedTableName(entityTypeId) + "_"
    definition.addColumns(
            ENTITY_SET_ID,
            ID_VALUE,
            ORIGIN_ID
    )
    definition.primaryKey(
            ENTITY_SET_ID,
            ID_VALUE
    )
    definition.addIndexes(
            PostgresExpressionIndexDefinition(definition, "(${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}')" )
                    .name(ApiUtil.dbQuote(indexPrefix + "origin_id"))
                    .ifNotExists()
                    .concurrent()
    )
    definition.addColumns(*propertyColumns.toTypedArray())
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
        " WHERE ${PARTITION.name} = ANY(?) " +
        "  AND ${ENTITY_SET_ID.name} = ANY(?) " +
        "  AND ${DataTables.LAST_WRITE.name} > ${transportTimestampColumn.name}" +
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

val pkCols = listOf(ENTITY_SET_ID, ID_VALUE)
val pk = pkCols.joinToString(", ") { it.name }
val ids = "$pk,${ORIGIN_ID.name}"

/**
 * column bindings are
 * 1 - partitions array
 * 2 - entity set ids array
 *
 */
fun updateOneBatchForProperty(
        destTable: String,
        propId: UUID,
        column: TransporterColumn
): String {
    val dataColumn = column.dataTableColumnName
    val destinationColumn = column.transporterTableColumnName
    // if data is deleted (version <= 0, use null, otherwise use the value from source
    val dataView = "CASE WHEN VERSION > 0 THEN $dataColumn else null END as $destinationColumn"

    val updateLastTransport = "UPDATE ${PostgresTable.DATA.name} " +
            "SET ${transportTimestampColumn.name} = ${LAST_WRITE.name} " +
            "WHERE ${PARTITION.name} = ANY(?) " +
            " AND ${ENTITY_SET_ID.name} = ANY(?) " +
            " AND ${PROPERTY_TYPE_ID.name} = '${propId}' " +
            " AND ${LAST_WRITE.name} > ${transportTimestampColumn.name} " +
            "RETURNING $ids,${VERSION.name},$dataView"

    // this seems unnecessary but it's good to be sure
    val createMissingRows = "INSERT INTO $destTable " +
            "($ids) " +
            "SELECT $ids " +
            "FROM src " +
            "WHERE ${VERSION.name} > 0 " +
            "ON CONFLICT ($pk) DO NOTHING"
    val modifyDestination = "UPDATE $destTable t " +
            "SET $destinationColumn = src.$destinationColumn " +
            "FROM src " +
            "WHERE " + pkCols.joinToString(" AND ") { "t." + it.name + " = src." + it.name}

    return "WITH src as ($updateLastTransport), inserted as ($createMissingRows) $modifyDestination"
}

/**
 * Update transported [destTable] entity type table
 *
 * column bindings are
 * 1 - partitions array
 * 2 - entity set ids array
 */
fun updatePrimaryKeyForEntitySets(destTable: String): String {
    val selectFromIds = "SELECT " +
            "${ENTITY_SET_ID.name},${ID_VALUE.name},${LINKING_ID.name}," +
            "${VERSION.name},${LAST_WRITE.name} " +
            "FROM ${PostgresTable.IDS.name} " +
            "WHERE ${PARTITION.name} = ANY(?) " +
            " AND ${ENTITY_SET_ID.name} = ANY(?) " +
            " AND ${LAST_WRITE.name} > ${transportTimestampColumn.name} LIMIT 10000"
    val createMissingRows = "INSERT INTO $destTable ($pk) " +
            "SELECT $pk " +
            "FROM src " +
            "WHERE ${VERSION.name} > 0 " +
            "ON CONFLICT ($pk) DO NOTHING"
    val createMissingLinkedRows = "INSERT INTO $destTable ($pk,${ORIGIN_ID.name}) " +
            "SELECT ${ENTITY_SET_ID.name}, " +
            " ${LINKING_ID.name} AS ${ID_VALUE.name}, " +
            " ${ID_VALUE.name} AS ${ORIGIN_ID.name} " +
            "FROM src " +
            "WHERE ${VERSION.name} > 0 " +
            " AND ${LINKING_ID.name} IS NOT NULL " +
            "ON CONFLICT ($pk) DO NOTHING"
    val deleteRows = "DELETE FROM $destTable t " +
            "USING src " +
            "WHERE src.${VERSION.name} <= 0 " +
            " AND t.${ENTITY_SET_ID.name} = src.${ENTITY_SET_ID.name} " +
            " AND t.${ID_VALUE.name} in (src.${ID_VALUE.name},src.${LINKING_ID.name}) " +
            "RETURNING src.${ID_VALUE.name}, src.${LAST_WRITE.name}"
    return "WITH src as ($selectFromIds), " +
            "inserts as ($createMissingRows)," +
            "insertLinks as ($createMissingLinkedRows) " +
            deleteRows
}

/**
 *
 */
fun updateOneBatchForEdges(): String {
    return ""
}

/**
 * Update [IDS] with new transport time
 *
 * column bindings are
 * 1 - partitions array
 * 2 - entity set ids array
 */
fun updateIdsForEntitySets(): String {
    return "UPDATE ${PostgresTable.IDS.name} " +
            "SET ${transportTimestampColumn.name} = ${DataTables.LAST_WRITE.name} " +
            "WHERE ${PARTITION.name} = ANY(?) " +
            " AND ${ENTITY_SET_ID.name} = ANY(?) " +
            " AND ${DataTables.LAST_WRITE.name} > ${transportTimestampColumn.name} "
}

fun addAllMissingColumnsQuery(table: PostgresTableDefinition): String =
        "ALTER TABLE ${table.name} " + table.columns.joinToString(",") { c -> "ADD COLUMN IF NOT EXISTS ${c.sql()}" }
