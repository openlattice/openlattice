package com.openlattice.postgres

import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.PARTITION

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
                "CREATE MATERIALIZED partition_counts IF NOT EXISTS AS select ${ENTITY_SET_ID.name},${PARTITION.name},count(*) from entity_key_ids GROUP BY (${ENTITY_SET_ID.name},${PARTITION.name})"
        )


    }
}