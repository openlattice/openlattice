package com.openlattice.organizations.external

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class SchemaMetadata(
        val name: String,
        val externalId: String,
        val tables: Set<TableMetadata>,
        val privileges: Map<String,Set<SchemaPrivilege>>
)