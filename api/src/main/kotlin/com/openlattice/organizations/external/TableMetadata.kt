package com.openlattice.organizations.external

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class TableMetadata(
        val name: String,
        val columns: List<ColumnMetadata>,
        val permissions: Map<String,Set<TablePrivilege>>
) {
}