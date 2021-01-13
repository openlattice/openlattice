package com.openlattice.organizations.external

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ColumnMetadata(
        val name: String,
        val permission: Map<String, Set<ColumnPrivilege>>
) {
}