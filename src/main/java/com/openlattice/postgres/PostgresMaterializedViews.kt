package com.openlattice.postgres

import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.PARTITION
import com.openlattice.postgres.PostgresTable.IDS

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class PostgresMaterializedViews {
    data class MaterializedView(
            val name: String,
            val createSql: String,
            val refreshSql: String = "REFRESH MATERIALIZED VIEW $name"
    )

    companion object {
        @JvmField
        val PARTITION_COUNTS = MaterializedView(
                "partition_counts",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS partition_counts AS " +
                        "SELECT ${ENTITY_SET_ID.name},${PARTITION.name},COUNT(*) " +
                        "FROM ${IDS.name} " +
                        "GROUP BY (${ENTITY_SET_ID.name},${PARTITION.name})"
        )


    }
}