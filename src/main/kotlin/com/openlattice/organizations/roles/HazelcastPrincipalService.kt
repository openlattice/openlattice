/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */
package com.openlattice.organizations.roles

import com.auth0.json.mgmt.users.User
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableSet
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.datastore.util.Util
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.processors.NestedPrincipalMerger
import com.openlattice.organizations.processors.NestedPrincipalRemover
import com.openlattice.organizations.roles.processors.PrincipalDescriptionUpdater
import com.openlattice.organizations.roles.processors.PrincipalTitleUpdater
import com.openlattice.principals.PrincipalExistsEntryProcessor
import com.openlattice.principals.RoleCreatedEvent
import com.openlattice.principals.UserCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class HazelcastPrincipalService(
        hazelcastInstance: HazelcastInstance,
        private val reservations: HazelcastAclKeyReservationService,
        private val authorizations: AuthorizationManager,
        private val eventBus: EventBus
) : SecurePrincipalsManager, AuthorizingComponent {

    private val principals = HazelcastMap.PRINCIPALS.getMap(hazelcastInstance)
    private val principalTrees = HazelcastMap.PRINCIPAL_TREES.getMap(hazelcastInstance)
    private val users = HazelcastMap.USERS.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory
                .getLogger(HazelcastPrincipalService::class.java)

        private fun findPrincipal(p: Principal): Predicate<AclKey, SecurablePrincipal> {
            return Predicates.equal(PrincipalMapstore.PRINCIPAL_INDEX, p)
        }

        private fun findPrincipals(principals: Collection<Principal>): Predicate<AclKey, SecurablePrincipal> {
            return Predicates.`in`(PrincipalMapstore.PRINCIPAL_INDEX, *principals.toTypedArray())
        }

        private fun hasSecurablePrincipal(principalAclKey: AclKey): Predicate<AclKey, AclKeySet> {
            return Predicates.equal("this.index[any]", principalAclKey.index)
        }

        private fun hasAnySecurablePrincipal(aclKeys: Set<AclKey>): Predicate<AclKey, AclKeySet> {
            return Predicates.`in`("this.index[any]", *aclKeys.map { it.index }.toTypedArray())
        }
    }

    override fun createSecurablePrincipalIfNotExists(owner: Principal, principal: SecurablePrincipal): Boolean {
        if (reservations.isReserved(principal.name)) {
            logger.warn("Securable Principal {} already exists", principal)
            return false
        }

        createSecurablePrincipal(owner, principal)
        return true
    }

    override fun createSecurablePrincipal(owner: Principal, principal: SecurablePrincipal) {
        val aclKey = principal.aclKey
        try {
            // Reserve securable object id
            reservations.reserveIdAndValidateType(principal, { principal.name })

            // Initialize entries in principals and principalTrees mapstores
            principals[aclKey] = principal
            principalTrees[aclKey] = AclKeySet()

            // Initialize permissions
            authorizations.setSecurableObjectType(aclKey, principal.category)
            authorizations.addPermission(aclKey, owner, EnumSet.allOf(Permission::class.java))

            // Post to EventBus if principal is a USER or ROLE
            when (principal.principalType) {
                PrincipalType.USER -> eventBus.post(UserCreatedEvent(principal))
                PrincipalType.ROLE -> eventBus.post(RoleCreatedEvent(principal as Role))
                else -> Unit
            }

        } catch (e: Exception) {
            logger.error("Unable to create principal {}", principal, e)
            Util.deleteSafely(principals, aclKey)
            Util.deleteSafely(principalTrees, aclKey)
            authorizations.deletePermissions(aclKey)
            reservations.release(principal.id)
            throw IllegalStateException("Unable to create principal: $principal")
        }
    }

    override fun updateTitle(aclKey: AclKey, title: String) {
        principals.executeOnKey(aclKey, PrincipalTitleUpdater(title))
    }

    override fun updateDescription(aclKey: AclKey, description: String) {
        principals.executeOnKey(aclKey, PrincipalDescriptionUpdater(description))
    }

    override fun getSecurablePrincipal(aclKey: AclKey): SecurablePrincipal? {
        return principals[aclKey]
    }

    override fun lookup(p: Principal): AclKey {
        return getFirstSecurablePrincipal(findPrincipal(p)).aclKey
    }

    override fun lookupRole(principal: Principal): Role {
        require(principal.type == PrincipalType.ROLE) { "The provided principal is not a role" }
        return getFirstSecurablePrincipal(findPrincipal(principal)) as Role
    }

    override fun getPrincipal(principalId: String): SecurablePrincipal {
        val id = Preconditions.checkNotNull(reservations.getId(principalId),
                "AclKey not found for Principal %s", principalId
        )
        return Util.getSafely(principals, AclKey(id))
    }

    override fun getAllRolesInOrganization(organizationId: UUID): Collection<SecurablePrincipal> {
        val rolesInOrganization = Predicates.and<AclKey, SecurablePrincipal>(
                Predicates.equal<AclKey, SecurablePrincipal>(PrincipalMapstore.PRINCIPAL_TYPE_INDEX, PrincipalType.ROLE),
                Predicates.equal<AclKey, SecurablePrincipal>(PrincipalMapstore.ACL_KEY_ROOT_INDEX, organizationId)
        )
        return principals.values(rolesInOrganization)
    }

    override fun deletePrincipal(aclKey: AclKey) {
        ensurePrincipalsExist(setOf(aclKey))
        authorizations.deletePrincipalPermissions(principals[aclKey]!!.principal)
        authorizations.deletePermissions(aclKey)
        principalTrees.executeOnEntries(NestedPrincipalRemover(setOf(aclKey)), hasSecurablePrincipal(aclKey))
        reservations.release(aclKey[aclKey.getSize() - 1])
        Util.deleteSafely(principalTrees, aclKey)
        Util.deleteSafely(principals, aclKey)
    }

    override fun deleteAllRolesInOrganization(organizationId: UUID) {
        getAllRolesInOrganization(organizationId).forEach { deletePrincipal(it.aclKey) }
    }

    override fun addPrincipalToPrincipal(source: AclKey, target: AclKey) {
        ensurePrincipalsExist(setOf(source, target))
        principalTrees.executeOnKey(target, NestedPrincipalMerger(ImmutableSet.of(source)))
    }

    override fun removePrincipalFromPrincipal(source: AclKey, target: AclKey) {
        removePrincipalsFromPrincipals(setOf(source), setOf(target))
    }

    override fun removePrincipalsFromPrincipals(source: Set<AclKey>, target: Set<AclKey>) {
        ensurePrincipalsExist(target + source)
        principalTrees.executeOnKeys(target, NestedPrincipalRemover(source))
    }

    override fun getAllPrincipalsWithPrincipal(aclKey: AclKey): Collection<SecurablePrincipal> {
        //We start from the bottom layer and use predicates to sweep up the tree and enumerate all roles with this role.
        var parentLayer = principalTrees.keySet(hasSecurablePrincipal(aclKey))
        val principalsWithPrincipal = parentLayer.toMutableSet()

        while (parentLayer.isNotEmpty()) {
            parentLayer = principalTrees.keySet(hasAnySecurablePrincipal(parentLayer))
            principalsWithPrincipal.addAll(parentLayer)
        }

        return principals.getAll(principalsWithPrincipal).values
    }

    override fun getSecurablePrincipals(p: Predicate<AclKey, SecurablePrincipal>): MutableCollection<SecurablePrincipal> {
        return principals.values(p)
    }

    override fun getParentPrincipalsOfPrincipal(aclKey: AclKey): Collection<SecurablePrincipal> {
        val parentLayer = principalTrees.keySet(hasSecurablePrincipal(aclKey))
        return principals.getAll(parentLayer).values
    }

    override fun principalHasChildPrincipal(parent: AclKey, child: AclKey): Boolean {
        return principalTrees[parent]?.contains(child) ?: false
    }

    override fun getAllUsersWithPrincipal(aclKey: AclKey): Collection<Principal> {
        return getAllPrincipalsWithPrincipal(aclKey)
                .filter { it.principalType == PrincipalType.USER }
                .map { it.principal }
                .toList()
    }

    override fun getAllUserProfilesWithPrincipal(principal: AclKey): Collection<User> {
        return users.getAll(getAllUsersWithPrincipal(principal).map { it.id }.toSet()).values
    }

    override fun getSecurablePrincipals(simplePrincipals: Collection<Principal>): Collection<SecurablePrincipal> {
        return principals.values(findPrincipals(simplePrincipals))
    }

    override fun principalExists(p: Principal): Boolean {
        return principals.keySet(Predicates.equal(PrincipalMapstore.PRINCIPAL_INDEX, p)).isNotEmpty()
    }

    override fun getUser(userId: String): User {
        return users.getValue(userId)
    }

    override fun getRole(organizationId: UUID, roleId: UUID): Role {
        val aclKey = AclKey(organizationId, roleId)
        return Util.getSafely(principals, aclKey) as Role
    }

    override fun getAllPrincipals(sp: SecurablePrincipal): Collection<SecurablePrincipal> {
        val roles = principalTrees[sp.aclKey] ?: return listOf()
        var nextLayer: Set<AclKey> = roles

        while (nextLayer.isNotEmpty()) {
            nextLayer = principalTrees.getAll(nextLayer)
                    .values
                    .flatten()
                    .filter { !roles.contains(it) }
                    .toSet()
            roles.addAll(nextLayer)
        }
        return principals.getAll(roles).values
    }

    override fun getAuthorizedPrincipalsOnSecurableObject(key: AclKey, permissions: EnumSet<Permission>): Set<Principal> {
        return authorizations.getAuthorizedPrincipalsOnSecurableObject(key, permissions)
    }

    private fun ensurePrincipalsExist(aclKeys: Set<AclKey>) {
        val principalsMap = principals.executeOnKeys(aclKeys, PrincipalExistsEntryProcessor())
        val nonexistentAclKeys = principalsMap.filterValues { !(it as Boolean) }.keys

        Preconditions.checkState(
                nonexistentAclKeys.isEmpty(),
                "All principals must exist, but principals with aclKeys [$nonexistentAclKeys] do not exist."
        )
    }

    private fun getFirstSecurablePrincipal(p: Predicate<AclKey, SecurablePrincipal>): SecurablePrincipal {
        return principals.values(p).first()
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }

    override fun getSecurablePrincipalById(id: UUID): SecurablePrincipal {
        return getFirstSecurablePrincipal(Predicates.equal(PrincipalMapstore.PRINCIPAL_ID_INDEX, id))
    }

    override fun getCurrentUserId(): UUID {
        return getPrincipal(Principals.getCurrentUser().id).id
    }
}