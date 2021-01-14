package com.openlattice.external

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ExternalSqlDatabaseTableMapping(
        val organizationExternalDatabaseTable: OrganizationExternalDatabaseTable,
        val tablePermissions: Map<AclKey, Set<Permission>>,
        val columnPermissions: Map<OrganizationExternalDatabaseColumn, Map<AclKey, Set<Permission>>>,

)