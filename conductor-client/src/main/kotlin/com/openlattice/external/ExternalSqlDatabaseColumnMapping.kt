package com.openlattice.external

import com.openlattice.authorization.Permission
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.OrganizationExternalDatabaseColumn

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ExternalSqlDatabaseColumnMapping(
        val organizationExternalDatabaseColumn: OrganizationExternalDatabaseColumn,
        val permissions: Map<SecurablePrincipal,Set<Permission>>

) {
}