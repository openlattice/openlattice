package com.openlattice.data.storage

import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTable.DATA

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries {
}


val dataTableColumnsSql = PostgresDataTables.dataTableColumns.joinToString(",")
val insertPropertyQuery = "INSERT INTO ${DATA.name} ($dataTableColumnsSql) VALUES "
