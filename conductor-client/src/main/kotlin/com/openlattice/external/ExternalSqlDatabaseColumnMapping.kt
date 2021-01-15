package com.openlattice.external

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principal
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.OrganizationExternalDatabaseColumn

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ExternalSqlDatabaseColumnMapping(
        val organizationExternalDatabaseColumn: OrganizationExternalDatabaseColumn,
        val permissions: Map<AclKey, Map<Principal, Set<Permission>>>
)