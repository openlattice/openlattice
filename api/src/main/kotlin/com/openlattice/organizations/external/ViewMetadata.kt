package com.openlattice.organizations.external

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ViewMetadata
/**
 * @param name The name of the view.
 * @param sql The sql defining the view
 * @param
 *
 */
constructor(
        val name: String,
        val sql: String,
        val stableId: String,
        val columns: List<ColumnMetadata>,
        val permissions: Map<String, Set<TablePrivilege>>
)