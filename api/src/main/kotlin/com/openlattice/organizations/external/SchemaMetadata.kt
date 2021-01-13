package com.openlattice.organizations.external

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SchemaMetadata(
        val name: String,
        val tables: Set<TableMetadata>,
        val permissions: Map<String,Set<SchemaPrivilege>>
) {
}