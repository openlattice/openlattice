package com.openlattice.transporter

import com.openlattice.ApiHelpers
import com.openlattice.IdConstants
import com.openlattice.edm.EdmConstants
import com.openlattice.postgres.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.transporter.types.TransporterColumn
import com.openlattice.transporter.types.TransporterDatastore.Companion.PUBLIC_SCHEMA
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import java.sql.Connection
import java.util.*

const val transporterNamespace = "transporter_data"

private val transportTimestampColumn: PostgresColumnDefinition = LAST_TRANSPORT

private const val BATCH_LIMIT = 10_000

val MAT_EDGES_TABLE = edgesTableDefinition()
val MAT_EDGES_COLUMNS_LIST = MAT_EDGES_TABLE.columns.map { it.name }.toList()
const val MAT_EDGES_TABLE_NAME = "et_edges"

fun unquotedTableName(entityTypeId: UUID): String {
    return "et_$entityTypeId"
}

internal fun tableName(entityTypeId: UUID): String {
    return ApiHelpers.dbQuote(unquotedTableName(entityTypeId))
}

internal fun tableNameWithSchema(schema: String, entityTypeId: UUID): String {
    return "${schema}.${tableName(entityTypeId)}"
}

fun edgesTableDefinition(): PostgresTableDefinition {
    val definition = PostgresTableDefinition("$PUBLIC_SCHEMA.$MAT_EDGES_TABLE_NAME")
    definition.addColumns(
            SRC_ENTITY_SET_ID,
            SRC_ENTITY_KEY_ID,
            DST_ENTITY_SET_ID,
            DST_ENTITY_KEY_ID,
            EDGE_ENTITY_SET_ID,
            EDGE_ENTITY_KEY_ID
    )
    definition.primaryKey(
            SRC_ENTITY_SET_ID,
            SRC_ENTITY_KEY_ID,
            DST_ENTITY_SET_ID,
            DST_ENTITY_KEY_ID,
            EDGE_ENTITY_SET_ID,
            EDGE_ENTITY_KEY_ID
    )
    return definition
}

fun tableDefinition(entityTypeId: UUID, propertyColumns: Collection<PostgresColumnDefinition>): PostgresTableDefinition {
    val definition = PostgresTableDefinition(tableNameWithSchema(PUBLIC_SCHEMA, entityTypeId))
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
            PostgresExpressionIndexDefinition(definition, "(${ORIGIN_ID.name} != '${IdConstants.EMPTY_ORIGIN_ID.id}')")
                    .name(ApiHelpers.dbQuote(indexPrefix + "origin_id"))
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
        "  AND abs(${VERSION.name}) > ${transportTimestampColumn.name}" +
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
 * 3 - entity key ids array
 */
fun updateRowsForPropertyType(
        destTable: String,
        propId: UUID,
        column: TransporterColumn
): String {
    val dataColumn = column.dataTableColumnName
    val destinationColumn = column.transporterTableColumnName
    // if data is deleted (version <= 0, use null, otherwise use the value from source
    val dataView = "CASE WHEN ${VERSION.name} > 0 THEN $dataColumn else null END as $destinationColumn"

    val updateLastTransport = "UPDATE ${PostgresTable.DATA.name} " +
            "SET ${transportTimestampColumn.name} = abs(${VERSION.name}) " +
            "WHERE ${PARTITION.name} = ANY(?) " +
            " AND ${ENTITY_SET_ID.name} = ANY(?) " +
            " AND ${ID_VALUE.name} = ANY(?) " +
            " AND ${PROPERTY_TYPE_ID.name} = '${propId}' " +
            " AND abs(${VERSION.name}) > ${transportTimestampColumn.name} " +
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
            "WHERE " + pkCols.joinToString(" AND ") { "t.${it.name} = src.${it.name}" }
    return "WITH src as ($updateLastTransport), " +
            "inserted as ($createMissingRows) " +
            modifyDestination
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
            "${ENTITY_SET_ID.name},${ID_VALUE.name},${LINKING_ID.name},${VERSION.name} " +
            "FROM ${PostgresTable.IDS.name} " +
            "WHERE ${PARTITION.name} = ANY(?) " +
            " AND ${ENTITY_SET_ID.name} = ANY(?) " +
            " AND abs(${VERSION.name}) > ${transportTimestampColumn.name} " +
            "LIMIT $BATCH_LIMIT"
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
            " AND t.${ID_VALUE.name} in (src.${ID_VALUE.name},src.${LINKING_ID.name}) "
    val results = "SELECT ${ID_VALUE.name}, abs(${VERSION.name}) as ${VERSION.name} FROM src"
    return "WITH src as ($selectFromIds), " +
            "inserts as ($createMissingRows)," +
            "insertLinks as ($createMissingLinkedRows), " +
            "deletes as ($deleteRows) " +
            results
}

/**
 * Transport edges
 *
 * column bindings are
 * 1 - partitions array
 * 2 - entity set ids array
 * 3 - entity key ids array
 * 4 - entity set ids array
 * 5 - entity key ids array
 * 6 - entity set ids array
 * 7 - entity key ids array
 */
fun updateRowsForEdges(): String {
    val pk = listOf(
            SRC_ENTITY_SET_ID,
            SRC_ENTITY_KEY_ID,
            DST_ENTITY_SET_ID,
            DST_ENTITY_KEY_ID,
            EDGE_ENTITY_SET_ID,
            EDGE_ENTITY_KEY_ID
    ).map { it.name }
    val pkString = pk.joinToString(",")

    val selectFromE = "UPDATE ${PostgresTable.E.name} " +
            "SET ${transportTimestampColumn.name} = abs(${VERSION.name}) " +
            "WHERE abs(${VERSION.name}) > ${transportTimestampColumn.name} AND (" +
            "  (" +
            "    ${PARTITION.name} = ANY(?) " +
            "    AND ${SRC_ENTITY_SET_ID.name} = ANY(?) " +
            "    AND ${SRC_ENTITY_KEY_ID.name} = ANY(?) " +
            "  ) OR (" +
            "    ${DST_ENTITY_SET_ID.name} = ANY(?) " +
            "    AND ${DST_ENTITY_KEY_ID.name} = ANY(?) " +
            "  ) OR (" +
            "    ${EDGE_ENTITY_SET_ID.name} = ANY(?) " +
            "    AND ${EDGE_ENTITY_KEY_ID.name} = ANY(?) " +
            "  ) " +
            ") RETURNING $pkString,${VERSION.name}"
    val createMissingRows = "INSERT INTO ${MAT_EDGES_TABLE.name} ($pkString) " +
            "SELECT $pkString " +
            "FROM src " +
            "WHERE ${VERSION.name} > 0 " +
            "ON CONFLICT ($pkString) DO NOTHING"
    val deleteRow = "DELETE FROM ${MAT_EDGES_TABLE.name} t " +
            "USING src " +
            "WHERE src.${VERSION.name} = 0 " +
            " AND ${
                pk.joinToString(" AND ") {
                    "t.${it} = src.${it}"
                }
            } "
    return "WITH src as ($selectFromE), " +
            "inserts as ($createMissingRows) " +
            deleteRow
}

/**
 * Update [IDS] with new transport time
 *
 * column bindings are
 * 1 - last write value being processed
 * 2 - partitions array
 * 3 - entity set ids array
 * 4 - entity key id
 */
fun updateLastWriteForId(): String {
    return "UPDATE ${PostgresTable.IDS.name} " +
            "SET ${transportTimestampColumn.name} = ? " +
            "WHERE ${PARTITION.name} = ANY(?) " +
            " AND ${ENTITY_SET_ID.name} = ANY(?) " +
            " AND ${ID_VALUE.name} = ? " +
            " AND abs(${VERSION.name}) > ${transportTimestampColumn.name} "
}

fun importTablesFromForeignSchemaQuery(
    remoteSchema: String,
    remoteTables: Set<String>,
    localSchema: String,
    usingFdwName: String
): String {
    val tablesClause = if (remoteTables.isEmpty()) {
        ""
    } else {
        "LIMIT TO (${remoteTables.joinToString(",")})"
    }
    return "import foreign schema $remoteSchema $tablesClause from server $usingFdwName INTO $localSchema;"
}

fun checkIfTableExistsQuery(
    schema: String,
    table: String
): String {
    return """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
                WHERE  table_schema = '$schema'
                AND    table_name   = '$table'
            )
        """.trimIndent()
}

fun addAllMissingColumnsQuery(table: PostgresTableDefinition, columns: Set<PostgresColumnDefinition>): String =
        "ALTER TABLE ${table.name} " + columns.joinToString(",") { c -> "ADD COLUMN IF NOT EXISTS ${c.sql()}" }

fun removeColumnsQuery(table: PostgresTableDefinition, columns: List<PostgresColumnDefinition>): String =
        "ALTER TABLE ${table.name} " + columns.joinToString(",") { c -> "DROP COLUMN ${c.name}" }

// TODO also need to refresh views when property types change (drop view, recreate view)
fun transportTable(
        table: PostgresTableDefinition,
        conn: Connection,
        logger: Logger,
        removedColumns: List<PostgresColumnDefinition> = listOf()
) {
    var lastSql = ""
    try {
        conn.createStatement().use { st ->
            lastSql = table.createTableQuery()
            st.execute(lastSql)
            lastSql = addAllMissingColumnsQuery(table, table.columns)
            st.execute(lastSql)

            if (removedColumns.isNotEmpty()) {
                lastSql = removeColumnsQuery(table, removedColumns)
                st.execute(lastSql)
            }
            table.createIndexQueries.forEach {
                lastSql = it
                st.execute(it)
            }
        }
    } catch (e: Exception) {
        logger.error("Unable to execute query: {}", lastSql, e)
        throw e
    }
}

fun setUserInhertRolePrivileges(role: String): String {
    return "ALTER ROLE ${ApiHelpers.dbQuote(role)} INHERIT"
}

fun revokeTablePermissionsForRole(schema: String, entitySetName: String, role: String): String {
    return "REVOKE ALL PRIVILEGES ON $schema.${ApiHelpers.dbQuote(entitySetName)} FROM ${ApiHelpers.dbQuote(role)}"
}

fun grantUsageOnschemaSql(schemaName: String, orgUserId: String): String {
    return "GRANT USAGE ON SCHEMA $schemaName TO ${ApiHelpers.dbQuote(orgUserId)}"
}

fun dropForeignTypeTable(schema: String, entityTypeId: UUID): String {
    return "DROP FOREIGN TABLE IF EXISTS $schema.${tableName(entityTypeId)}"
}

fun destroyEdgeView(schema: String, entitySetName: String): String {
    return "DROP VIEW IF EXISTS $schema.${ApiHelpers.dbQuote(edgeViewName(entitySetName))}"
}

fun destroyView(schema: String, entitySetName: String): String {
    return "DROP VIEW IF EXISTS $schema.${ApiHelpers.dbQuote(entitySetName)}"
}

fun createEntitySetViewInSchemaFromSchema(
        entitySetName: String,
        entitySetId: UUID,
        destinationSchema: String,
        entityTypeId: UUID,
        propertyTypes: Map<UUID, FullQualifiedName>,
        sourceSchema: String
): String {
    val colsSql = propertyTypes.map { (id, ptName) ->
        val column = ApiHelpers.dbQuote(id.toString())
        val quotedPt = ApiHelpers.dbQuote(ptName.toString())
        "$column as $quotedPt"
    }.joinToString()

    return """
            CREATE OR REPLACE VIEW $destinationSchema.${ApiHelpers.dbQuote(entitySetName)} AS 
                SELECT ${ID_VALUE.name} as ${ApiHelpers.dbQuote(EdmConstants.ID_FQN.toString())}, 
                    $colsSql FROM $sourceSchema.${tableName(entityTypeId)}
                WHERE ${ENTITY_SET_ID.name} = '$entitySetId'
        """.trimIndent()
}

fun createEdgeSetViewInSchema(
        entitySetName: String,
        entitySetId: UUID,
        destinationSchema: String,
        sourceSchema: String
): String {
    return """
        CREATE OR REPLACE VIEW $destinationSchema.${ApiHelpers.dbQuote(edgeViewName(entitySetName))} AS
            SELECT * FROM $sourceSchema.${MAT_EDGES_TABLE_NAME}
            WHERE ${SRC_ENTITY_SET_ID.name} = '$entitySetId'
            OR ${DST_ENTITY_SET_ID.name} = '$entitySetId'
            OR ${EDGE_ENTITY_SET_ID.name} = '$entitySetId'
    """.trimIndent()
}

fun grantRoleToUser(roleName: String, username: String): String {
    return "GRANT ${ApiHelpers.dbQuote(roleName)} to ${ApiHelpers.dbQuote(username)}"
}

fun grantSelectOnColumnsToRoles(schema: String, entitySetName: String, role: String, columns: List<String>): String {
    if (columns.isEmpty()) {
        return ""
    }
    val columnsSql = columns.joinToString {
        ApiHelpers.dbQuote(it)
    }

    return "GRANT SELECT ( $columnsSql ) ON $schema.${ApiHelpers.dbQuote(entitySetName)} TO ${ApiHelpers.dbQuote(role)}"
}

/**
 * The below are intentionally unquoted
 */
fun edgeViewName(entitySetName: String): String {
    return "${entitySetName}_edges"
}

fun viewRoleName(entitySetName: String, columnName: String): String {
    return "${entitySetName}_$columnName"
}