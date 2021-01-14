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
        val externalId: String = name,
        val schema: String,
        val comment: String,
        val columns: List<ColumnMetadata>,
        val privileges: Map<String,Set<TablePrivilege>>,
        val lastUpdated: OffsetDateTime
)