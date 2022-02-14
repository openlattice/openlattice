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
import com.google.common.collect.Sets
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.authorization.mapstores.PrincipalTreesMapstore
import com.openlattice.datastore.util.Util
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.processors.NestedPrincipalRemover
import com.openlattice.organizations.roles.processors.PrincipalDescriptionUpdater
import com.openlattice.organizations.roles.processors.PrincipalTitleUpdater
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.principals.AddPrincipalToPrincipalEntryProcessor
import com.openlattice.principals.PrincipalExistsEntryProcessor
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

/**
 * Principals service is an API for performing composite operations on principals in the system.
 */
@Service
class HazelcastPrincipalService(
        hazelcastInstance: HazelcastInstance,
        private val reservations: HazelcastAclKeyReservationService,
        private val authorizations: AuthorizationManager,
        private val principalsMapManager: PrincipalsMapManager,
        private val extDatabasePermsManager: ExternalDatabasePermissioningService
) : SecurePrincipalsManager, AuthorizingComponent {

    private val principals = HazelcastMap.PRINCIPALS.getMap(hazelcastInstance)
    private val principalTrees = HazelcastMap.PRINCIPAL_TREES.getMap(hazelcastInstance)
    private val users = HazelcastMap.USERS.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory
                .getLogger(HazelcastPrincipalService::class.java)

        private fun findPrincipal(p: Principal): Predicate<AclKey, SecurablePrincipal> {
            return PrincipalsMapManager.findPrincipal(p)
        }

        private fun findPrincipals(principals: Collection<Principal>): Predicate<AclKey, SecurablePrincipal> {
            return Predicates.`in`(PrincipalMapstore.PRINCIPAL_INDEX, *principals.toTypedArray())
        }

        private fun hasPrincipalType(principalType: PrincipalType): Predicate<AclKey, SecurablePrincipal> {
            return PrincipalsMapManager.hasPrincipalType(principalType)
        }

        private fun hasSecurablePrincipal(principalAclKey: AclKey): Predicate<AclKey, AclKeySet> {
            return Predicates.equal(PrincipalTreesMapstore.INDEX, principalAclKey.index)
        }

        private fun hasAnySecurablePrincipal(aclKeys: Set<AclKey>): Predicate<AclKey, AclKeySet> {
            return Predicates.`in`(PrincipalTreesMapstore.INDEX, *aclKeys.map { it.index }.toTypedArray())
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

    private fun createSecurablePrincipal(owner: Principal, principal: SecurablePrincipal) {
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

            when (principal.principalType) {
                PrincipalType.USER -> extDatabasePermsManager.createUnprivilegedUser(principal)
                PrincipalType.ROLE -> extDatabasePermsManager.createRole(principal as Role)
            }
        } catch (e: Exception) {
            logger.error("Unable to create principal {}", principal, e)
            principals.delete(aclKey)
            principalTrees.delete(aclKey)
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
        return principalsMapManager.getSecurablePrincipal(aclKey)
    }

    override fun lookup(p: Principal): AclKey {
        return getFirstSecurablePrincipal(findPrincipal(p)).aclKey
    }

    override fun lookup(p: MutableSet<Principal>): MutableMap<Principal, AclKey> {
        return principals.entrySet(findPrincipals(p)).associate { it.value.principal to it.key }.toMutableMap()
    }

    override fun lookupRole(principal: Principal): Role {
        return principalsMapManager.lookupRole(principal)
    }

    override fun getSecurablePrincipal(principalId: String): SecurablePrincipal {
        return principalsMapManager.getSecurablePrincipal(principalId)
    }

    override fun getSecurablePrincipals(aclKeys: Set<AclKey>): Map<AclKey, SecurablePrincipal> {
        return principalsMapManager.getSecurablePrincipals(aclKeys)
    }

    override fun getAllRolesInOrganization(organizationId: UUID): Collection<SecurablePrincipal> {
        val roles = getAllRolesInOrganizations(listOf(organizationId))
        if (roles.isEmpty() || roles.containsKey(organizationId).not()) {
            logger.error("no roles exist for organization {}", organizationId)
            return emptyList()
        }
        return roles.getValue(organizationId)
    }

    override fun getAllRolesInOrganizations(organizationIds: Collection<UUID>): Map<UUID, Collection<SecurablePrincipal>> {
        val rolesInOrganization = Predicates.and<AclKey, SecurablePrincipal>(
                hasPrincipalType(PrincipalType.ROLE),
                Predicates.`in`<AclKey, SecurablePrincipal>(PrincipalMapstore.ACL_KEY_ROOT_INDEX, *organizationIds.toTypedArray())
        )
        return principals.values(rolesInOrganization).groupBy { it.aclKey[0] }
    }

    override fun deletePrincipal(aclKey: AclKey) {
        ensurePrincipalsExist(setOf(aclKey))
        authorizations.deletePrincipalPermissions(principals[aclKey]!!.principal)
        authorizations.deletePermissions(aclKey)
        principalTrees.executeOnEntries(NestedPrincipalRemover(setOf(aclKey)), hasSecurablePrincipal(aclKey))
        reservations.release(aclKey[aclKey.getSize() - 1])
        principalTrees.delete(aclKey)
        principals.delete(aclKey)
    }

    override fun deleteAllRolesInOrganization(organizationId: UUID) {
        getAllRolesInOrganization(organizationId).forEach { deletePrincipal(it.aclKey) }
    }

    override fun addPrincipalToPrincipal(source: AclKey, target: AclKey) {
        addPrincipalToPrincipals(source, setOf(target))
    }

    override fun addPrincipalToPrincipals(source: AclKey, targets: Set<AclKey>): Set<AclKey> {
        ensurePrincipalsExist(targets + setOf(source))

        logger.debug("about to add principal $source to each of ${targets.joinToString()}")
        val updatedKeys = principalTrees
                .executeOnKeys(targets, AddPrincipalToPrincipalEntryProcessor(source))
                .values
                .filterNotNull()
                .toSet()

        // consider renaming to updateExternalPrincipalTrees
        extDatabasePermsManager.addPrincipalToPrincipals(source, updatedKeys)
        return updatedKeys
    }

    override fun removePrincipalFromPrincipal(source: AclKey, target: AclKey) {
        removePrincipalsFromPrincipals(setOf(source), setOf(target))
    }

    override fun removePrincipalsFromPrincipals(principalsToRemove: Set<AclKey>, fromPrincipals: Set<AclKey>) {
        ensurePrincipalsExist(fromPrincipals + principalsToRemove)
        principalTrees.executeOnKeys(fromPrincipals, NestedPrincipalRemover(principalsToRemove))
        extDatabasePermsManager.removePrincipalsFromPrincipals(principalsToRemove, fromPrincipals)
    }

    private fun getAllPrincipalsWithPrincipal(aclKey: AclKey): Collection<SecurablePrincipal> {
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
        return getParentPrincipalsOfPrincipals(setOf(aclKey)).getValue(aclKey)
    }

    private fun getParentPrincipalsOfPrincipals(aclKeys: Set<AclKey>): Map<AclKey, Collection<SecurablePrincipal>> {
        val parentLayers = principalTrees.entrySet(hasAnySecurablePrincipal(aclKeys))
        val principals = principals.getAll(parentLayers.map { it.key }.toSet())
        val childrenToParents = mutableMapOf<AclKey, MutableSet<SecurablePrincipal>>()
        parentLayers.forEach { (parent, children) ->
            val sp = principals[parent] ?: return@forEach

            children.filter { aclKeys.contains(it) }.forEach { childAclKey ->
                if (!childrenToParents.containsKey(childAclKey)) {
                    childrenToParents[childAclKey] = mutableSetOf()
                }

                childrenToParents.getValue(childAclKey).add(sp)
            }
        }

        return childrenToParents
    }

    override fun getOrganizationMembers(organizationIds: MutableSet<UUID>): Map<UUID, Set<SecurablePrincipal>> {
        val orgAclKeys = organizationIds.map { AclKey(it) }.toSet()
        val orgMembers = getParentPrincipalsOfPrincipals(orgAclKeys)
        return orgAclKeys
                .associate {
                    it.first() to orgMembers.getOrDefault(it, setOf()).filter { p -> p.principalType == PrincipalType.USER }.toMutableSet()
                }
    }


    override fun getOrganizationMemberPrincipals(organizationId: UUID): Set<Principal> {
        return getOrganizationMembers(mutableSetOf(organizationId))
                .getValue(organizationId)
                .map { it.principal }
                .toSet()
    }

    override fun principalHasChildPrincipal(parent: AclKey, child: AclKey): Boolean {
        return principalTrees[parent]?.contains(child) ?: false
    }

    override fun getAllUsersWithPrincipal(aclKey: AclKey): Collection<SecurablePrincipal> {
        return getAllPrincipalsWithPrincipal(aclKey).filter { it.principalType == PrincipalType.USER }
    }

    override fun getAllUserProfilesWithPrincipal(principal: AclKey): Collection<User> {
        return users.getAll(getAllUsersWithPrincipal(principal).map { it.principal.id }.toSet()).values
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

    override fun bulkGetUnderlyingPrincipals(sps: Set<SecurablePrincipal>): Map<SecurablePrincipal, Set<Principal>> {
        val aclKeyPrincipals = mutableMapOf<AclKey, AclKeySet>()

        // Bulk load all relevant principal trees from hazelcast
        var nextLayer = sps.mapTo(mutableSetOf()) { it.aclKey }
        while (nextLayer.isNotEmpty()) {
            //Don't load what's already been loaded.
            val nextLayerMap = principalTrees.getAll(nextLayer - aclKeyPrincipals.keys)
            nextLayer = nextLayerMap.values.flatMapTo(mutableSetOf()) { it.value }
            aclKeyPrincipals.putAll(nextLayerMap)
        }

        // Map all loaded principals to SecurablePrincipals
        val aclKeysToPrincipals = principals.getAll(aclKeyPrincipals.keys + aclKeyPrincipals.values.flatten())

        // Map each SecurablePrincipal to all its aclKey children from the in-memory map, and from there a SortedPrincipalSet
        return sps.associateWith { sp ->
            val childAclKeys = mutableSetOf<AclKey>(sp.aclKey) //Need to include self.
            aclKeyPrincipals.getOrDefault(sp.aclKey, AclKeySet()).forEach { childAclKeys.add(it) }

            var nextAclKeyLayer: Set<AclKey> = childAclKeys

            while (nextAclKeyLayer.isNotEmpty()) {
                nextAclKeyLayer = (nextAclKeyLayer.flatMapTo(mutableSetOf<AclKey>()) {
                    aclKeyPrincipals[it] ?: setOf()
                }) - childAclKeys
                childAclKeys += nextAclKeyLayer
            }

            val principals = childAclKeys.mapNotNullTo(Sets.newLinkedHashSetWithExpectedSize(childAclKeys.size)) { aclKey ->
                aclKeysToPrincipals[aclKey]?.principal
            }

            if (childAclKeys.size != principals.size) {
                logger.warn("Unable to retrieve principals for acl keys: ${childAclKeys - aclKeysToPrincipals.keys}")
            }

            principals
        }
    }

    override fun ensurePrincipalsExist(aclKeys: Set<AclKey>) {
        val principalsMap = principals.executeOnKeys(aclKeys, PrincipalExistsEntryProcessor())
        val nonexistentAclKeys = principalsMap.filterValues { !it }.keys

        Preconditions.checkState(
                nonexistentAclKeys.isEmpty(),
                "All principals must exist, but principals with aclKeys $nonexistentAclKeys do not exist."
        )
    }

    private fun getFirstSecurablePrincipal(p: Predicate<AclKey, SecurablePrincipal>): SecurablePrincipal {
        return PrincipalsMapManager.getFirstSecurablePrincipal(principals, p)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }

    override fun getCurrentUserId(): UUID {
        return getSecurablePrincipal(Principals.getCurrentUser().id).id
    }

    override fun getAllRoles(): Set<Role> {
        return principalsMapManager.getAllRoles()
    }

    override fun getAllUsers(): Set<SecurablePrincipal> {
        return principals.values(hasPrincipalType(PrincipalType.USER)).toSet()
    }
}
