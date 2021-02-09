package com.openlattice.authorization

import com.hazelcast.map.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.organization.roles.Role

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
interface PrincipalsMapManager {
    companion object {
        fun findPrincipal(p: Principal): Predicate<AclKey, SecurablePrincipal> {
            return Predicates.equal(PrincipalMapstore.PRINCIPAL_INDEX, p)
        }

        fun hasPrincipalType(principalType: PrincipalType): Predicate<AclKey, SecurablePrincipal> {
            return Predicates.equal<AclKey, SecurablePrincipal>(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, principalType)
        }

        fun getFirstSecurablePrincipal(principals: IMap<AclKey, SecurablePrincipal>, p: Predicate<AclKey, SecurablePrincipal>): SecurablePrincipal {
            return principals.values(p).first()
        }
    }

    fun lookupRole(aclKey: AclKey): Role

    fun lookupRole(principal: Principal): Role

    fun getAllRoles(): Set<Role>

    fun getSecurablePrincipal(principalId: String): SecurablePrincipal

    fun getSecurablePrincipal(aclKey: AclKey): SecurablePrincipal?

    fun getSecurablePrincipals(aclKeys: Set<AclKey>): Map<AclKey, SecurablePrincipal>

}
