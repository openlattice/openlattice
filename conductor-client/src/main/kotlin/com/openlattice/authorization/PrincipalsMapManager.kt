package com.openlattice.authorization

import com.openlattice.organization.roles.Role

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
interface PrincipalsMapManager {
    fun lookupRole(aclKey: AclKey): Role

    fun lookupRole(principal: Principal): Role

    fun getAllRoles(): Set<Role>

    fun getSecurablePrincipal(principalId: String): SecurablePrincipal

    fun getSecurablePrincipal(aclKey: AclKey): SecurablePrincipal?
}
