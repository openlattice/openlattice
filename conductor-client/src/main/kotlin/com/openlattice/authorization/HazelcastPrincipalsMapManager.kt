package com.openlattice.authorization

import com.google.common.base.Preconditions
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.roles.Role

/**
 * Principals manager manages the principals map
 *
 * @author Drew Bailey (drew@openlattice.com)
 */
class HazelcastPrincipalsMapManager(
        hazelcastInstance: HazelcastInstance,
        private val reservations: HazelcastAclKeyReservationService
): PrincipalsMapManager {

    private val principals = HazelcastMap.PRINCIPALS.getMap(hazelcastInstance)

    override fun lookupRole(aclKey: AclKey): Role {
        val principal = principals.getValue(aclKey)
        return lookupRole(principal.principal)
    }

    override fun lookupRole(principal: Principal): Role {
        require(principal.type == PrincipalType.ROLE) { "The provided principal $principal is not a role" }
        return getFirstSecurablePrincipal(findPrincipal(principal)) as Role
    }

    override fun getAllRoles(): Set<Role> {
        return principals.values(hasPrincipalType(PrincipalType.ROLE)).map { it as Role }.toSet()
    }

    override fun getSecurablePrincipal(principalId: String): SecurablePrincipal {
        val id = Preconditions.checkNotNull(reservations.getId(principalId),
                "AclKey not found for Principal %s", principalId
        )
        return principals.getValue(AclKey(id))
    }

    override fun getSecurablePrincipal(aclKey: AclKey): SecurablePrincipal? {
        return principals[aclKey]
    }

    private fun hasPrincipalType(principalType: PrincipalType): Predicate<AclKey, SecurablePrincipal> {
        return Predicates.equal<AclKey, SecurablePrincipal>(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, principalType)
    }

    private fun findPrincipal(p: Principal): Predicate<AclKey, SecurablePrincipal> {
        return Predicates.equal(PrincipalMapstore.PRINCIPAL_INDEX, p)
    }

    private fun getFirstSecurablePrincipal(p: Predicate<AclKey, SecurablePrincipal>): SecurablePrincipal {
        return principals.values(p).first()
    }
}