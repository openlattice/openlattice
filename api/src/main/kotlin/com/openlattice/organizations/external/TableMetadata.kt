package com.openlattice.organizations.external

import java.time.OffsetDateTime

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class TableMetadata
/**
 * @param privileges A map from roles to the set of privileges they have on the table.
 */
constructor(
        val name: String,
        val schema: String,
        val externalId: String = "$schema.$name",
        val comment: String,
        val columns: MutableList<ColumnMetadata>,
        val privileges: MutableMap<String, Set<TablePrivilege>>,
        val lastUpdated: OffsetDateTime
) {
    val tableKey = TableKey(name, schema, externalId)
}

data class TableKey(
        val name: String,
        val schema: String,
        val externalId: String = "$schema.$name"
)