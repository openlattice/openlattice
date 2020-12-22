package com.openlattice.postgres.external

import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Action
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.roles.Role
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
interface ExternalDatabasePermissioningService {

    /**
     * Creates [role] in the configured external database
     */
    fun createRole(role: Role)

    /**
     * Creates user [principal] in the configured external database
     */
    fun createUnprivilegedUser(principal: SecurablePrincipal)

    /**
     * Adds [sourcePrincipalAclKey] to [targetPrincipalAclKeys]
     */
    fun addPrincipalToPrincipals(sourcePrincipalAclKey: AclKey, targetPrincipalAclKeys: Set<AclKey>)

    /**
     * Removes [principalToRemove] from [fromPrincipals]
     */
    fun removePrincipalsFromPrincipals(principalsToRemove: Set<AclKey>, fromPrincipals: Set<AclKey>)

    /**
     * Initializes permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    fun initializeAssemblyPermissions(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entitySetName: String,
            propertyTypes: Set<PropertyTypeIdFqn>
    )

    /**
     * Updates permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    fun updateAssemblyPermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<UUID, OrganizationExternalDatabaseColumn>
    )

    /**
     * Initializes permissions on [columns] for [table] in org database for [organizationId]
     */
    fun initializeExternalTablePermissions(
            organizationId: UUID,
            table: OrganizationExternalDatabaseTable,
            columns: Set<OrganizationExternalDatabaseColumn>
    )

    /**
     * Updates permissions on [columns] for [table] in org database for [organizationId]
     */
    fun updateExternalTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<UUID, OrganizationExternalDatabaseColumn>
    )
}