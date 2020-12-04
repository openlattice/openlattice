package com.openlattice.postgres.external

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role

/**
 * @author Drew Bailey (drewbaileym@gmail.com)
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
}