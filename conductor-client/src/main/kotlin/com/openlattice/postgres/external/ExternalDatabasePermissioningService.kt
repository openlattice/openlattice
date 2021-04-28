package com.openlattice.postgres.external

import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Action
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.TableColumn
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
     * Sets privileges a user on an organization's columns
     */
    fun executePrivilegesUpdate(action: Action, acls: List<Acl>)

    /**
     * Adds [sourcePrincipalAclKey] to [targetPrincipalAclKeys]
     */
    fun addPrincipalToPrincipals(sourcePrincipalAclKey: AclKey, targetPrincipalAclKeys: Set<AclKey>)

    /**
     * Removes [principalToRemove] from [fromPrincipals]
     */
    fun removePrincipalsFromPrincipals(principalsToRemove: Set<AclKey>, fromPrincipals: Set<AclKey>)

    /**
     * Updates permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    fun updateAssemblyPermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    )

    /**
     * Updates permissions on [columns] for [table] in org database for [organizationId]
     */
    fun updateExternalTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    )
}