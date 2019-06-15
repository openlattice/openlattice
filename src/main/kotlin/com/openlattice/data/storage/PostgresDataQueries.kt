package com.openlattice.data.storage

import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable.DATA

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries {
}


val dataTableColumnsSql = PostgresDataTables.dataTableColumns.joinToString(",") { it.name }
val dataTableColumnsBindSql = PostgresDataTables.dataTableColumns.joinToString(",") { "?" }
val dataTableColumnsConflictSetSql = PostgresDataTables.dataTableColumns.joinToString(",") { "${it.name} = EXCLUDED.${it.name}" }
val insertPropertyQuery = "INSERT INTO ${DATA.name} ($dataTableColumnsSql) VALUES ($dataTableColumnsBindSql) ON CONFLICT " +
        "DO UPDATE SET $dataTableColumnsConflictSetSql"


