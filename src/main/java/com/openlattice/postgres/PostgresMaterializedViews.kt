package com.openlattice.postgres

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class PostgresMaterializedViews {
    data class MaterializedView( val name: String,
                                 val createSql : String,
                                 val refresh: String = "REFRESH MATERIALIZED VIEW $name" )

    companion object {
        @JvmField
        val PARTITION_COUNTS = MaterializedView(
                "partition_loads",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS"
        )



    }
}