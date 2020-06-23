package com.openlattice.postgres

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
}