package com.openlattice.organizations.external

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ColumnMetadata(
        val name: String,
        val externalId: String = name,
        val schema: String,
        val sqlDataType: String,
        val isPrimaryKey: Boolean,
        val isNullable: Boolean,
        val ordinalPosition: Int,
        val privileges: Map<String, Set<ColumnPrivilege>>,
        val maskingPolicy: String
)