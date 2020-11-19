package com.openlattice.organizations

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions
import com.google.common.collect.Iterables
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.Assembler
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.PrincipalMapstore
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.notifications.sms.PhoneNumberService
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.organization.OrganizationPrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.events.*
import com.openlattice.organizations.mapstores.CONNECTIONS_INDEX
import com.openlattice.organizations.mapstores.MEMBERS_INDEX
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import com.openlattice.organizations.processors.OrganizationEntryProcessor.Result
import com.openlattice.organizations.processors.OrganizationReadEntryProcessor
import com.openlattice.organizations.processors.UpdateOrganizationSmsEntitySetInformationEntryProcessor
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.openlattice.users.getAppMetadata
import com.openlattice.users.processors.aggregators.UsersWithConnectionsAggregator
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream
import javax.inject.Inject

/**
 * This class manages organizations.
 *
 *
 * An organization is a collection of principals and applications.
 *
 *
 * Access to organizations is handled by the organization manager.
 *
 *
 * Membership in an organization is stored in the membersOf field which is accessed via an IMAP. Only principals of type
 * [PrincipalType.USER]. This is mainly because we don't store the principal type field along with the principal id.
 * This may change in the future.
 *
 *
 * While roles may create organizations they cannot be members. That is an organization created by a role will have no members
 * but principals with that role will have the relevant level of access to that role. In addition, roles that create an
 * organization will not inherit the organization role (as they are not members).
 */
@Service
class HazelcastOrganizationService(
        hazelcastInstance: HazelcastInstance,
        private val reservations: HazelcastAclKeyReservationService,
        private val authorizations: AuthorizationManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val phoneNumbers: PhoneNumberService,
        private val partitionManager: PartitionManager,
        private val assembler: Assembler,
        private val organizationMetadataEntitySetsService: OrganizationMetadataEntitySetsService
) {
    init {
        organizationMetadataEntitySetsService.organizationService = this
    }

    protected val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)
    protected val users = HazelcastMap.USERS.getMap(hazelcastInstance)
    protected val organizationDatabases = HazelcastMap.ORGANIZATION_DATABASES.getMap(hazelcastInstance)

    @Inject
    private lateinit var eventBus: EventBus

    @Timed
    fun getOrganization(p: Principal): OrganizationPrincipal {
        return checkNotNull(securePrincipalsManager.getPrincipal(p.id) as OrganizationPrincipal)
    }

    @Timed
    fun getOrganizationPrincipals(ids: Set<UUID>): Collection<OrganizationPrincipal> {
        return organizations.executeOnKeys(ids) {
            it.value.securablePrincipal
        }.values
    }

    @Timed
    fun maybeGetOrganization(p: Principal): Optional<SecurablePrincipal> {
        return try {
            Optional.of(securePrincipalsManager.getPrincipal(p.id))
        } catch (e: NullPointerException) {
            Optional.empty()
        }
    }

    @Timed
    fun getAllOrganizations(): Iterable<Organization> {
        return organizations.values
    }

    private fun initializeOrganizationPrincipal(principal: Principal, organization: Organization) {
        require(
                securePrincipalsManager.createSecurablePrincipalIfNotExists(
                        principal,
                        organization.securablePrincipal
                )
        ) {
            "Unable to create securable principal for organization. This means the organization probably already exists."
        }
    }

    private fun initializeOrganizationAdminRole(principal: Principal, adminRoleAclKey: AclKey, organization: Organization): Role {
        //Create the admin role for the organization and give it ownership of organization.
        val adminRole = createOrganizationAdminRole(organization.securablePrincipal, adminRoleAclKey)
        createRoleIfNotExists(principal, adminRole)
        authorizations.addPermission(organization.getAclKey(), adminRole.principal, EnumSet.allOf(Permission::class.java))
        addRoleToPrincipalInOrganization(organization.id, adminRole.id, principal)

        //Grant the creator of the organizations
        authorizations.addPermission(organization.getAclKey(), principal, EnumSet.allOf(Permission::class.java))

        return adminRole
    }

    @Timed
    fun createOrganization(principal: Principal, organization: Organization) {
        /*
         * Roles shouldn't be members of an organizations.
         *
         * Membership is currently defined as your principal having the
         * organization principal.
         *
         * In order to function roles must have READ access on the organization and
         */
        val membersToAdd = when (principal.type) {
            PrincipalType.USER ->
                //Add the organization principal to the creator marking them as a member of the organization
                setOf(principal)
            PrincipalType.ROLE ->
                //For a role we ensure that it has
                setOf()
            else -> throw IllegalStateException("Only users and roles can create organizations.")
        }//Fall through by design

        initializeOrganizationPrincipal(principal, organization)
        initializeOrganization(organization)

        // set up organization database
        val orgDatabase = assembler.createOrganizationAndReturnOid(organization.id)
        organizationDatabases.set(organization.id, orgDatabase)

        val adminRole = initializeOrganizationAdminRole(principal, organization.adminRoleAclKey, organization)

        organizationMetadataEntitySetsService.initializeOrganizationMetadataEntitySets(adminRole)

        if (membersToAdd.isNotEmpty()) {
            addMembers(organization.getAclKey().first(), membersToAdd, mapOf())
        }

        eventBus.post(OrganizationCreatedEvent(organization))
        setSmsEntitySetInformation(organization.smsEntitySetInfo)
    }

    private fun initializeOrganization(organization: Organization) {
        val organizationId = organization.securablePrincipal.id
        if (organization.partitions.isEmpty()) {
            organization.partitions.addAll(partitionManager.allocateDefaultOrganizationPartitions(organizationId))
        }

        organizations.set(organizationId, organization)
    }

    @Timed
    fun getOrganization(organizationId: UUID): Organization? {
        val org = organizations[organizationId]
        val roles = getRoles(organizationId)
        val members = getMembers(organizationId)

        org?.roles?.addAll(roles)
        org?.members?.addAll(members)

        return org
    }

    @Timed
    fun getOrganizationApps(organizationId: UUID): Set<UUID> {
        return organizations.executeOnKey(organizationId, OrganizationReadEntryProcessor {
            DelegatedUUIDSet.wrap(it.apps)
        }) as Set<UUID>
    }

    @Timed
    fun getAdminRoleAclKey(organizationId: UUID): AclKey {
        return organizations.executeOnKey(organizationId, OrganizationReadEntryProcessor {
            it.adminRoleAclKey
        }) as AclKey
    }

    fun getOrganizationDatabaseName(organizationId: UUID): String {
        return organizationDatabases.getValue(organizationId).name
    }

    @Deprecated("Calls to load multiple organizations at once should use OrganizationService.getOrganizationPrincipals. " +
            "Organization members (and other data) should only be loaded per organization as needed.")
    @JvmOverloads
    fun getOrganizations(organizationIdStream: Stream<UUID>, includeMembers: Boolean = false): Iterable<Organization> {
        //TODO: Figure out why copy is here?

        val orgIds = organizationIdStream.collect(Collectors.toSet())

        val membersByOrg = if (includeMembers) {
            securePrincipalsManager.getOrganizationMembers(orgIds).mapValues { it.value.map { sp -> sp.principal }.toMutableSet() }
        } else {
            mapOf()
        }

        return organizations.getAll(orgIds)
                .values
                .filterNotNull()
                .map {
                    Organization(
                            it.securablePrincipal,
                            it.adminRoleAclKey,
                            it.emailDomains,
                            membersByOrg[it.id] ?: mutableSetOf(),
                            //TODO: If you're an organization you can view its roles.
                            it.roles,
                            it.smsEntitySetInfo,
                            it.partitions,
                            it.apps
                    )
                }
    }

    fun destroyOrganization(organizationId: UUID) {
        // Remove all roles
        val aclKey = AclKey(organizationId)
        authorizations.deletePermissions(aclKey)
        securePrincipalsManager.deleteAllRolesInOrganization(organizationId)
        securePrincipalsManager.deletePrincipal(aclKey)
        organizations.delete(organizationId)
        assembler.destroyOrganization(organizationId)
        organizationDatabases.delete(organizationId)
        reservations.release(organizationId)
        eventBus.post(OrganizationDeletedEvent(organizationId))
    }

    @Timed
    fun updateTitle(organizationId: UUID, title: String) {
        val aclKey = AclKey(organizationId)
        securePrincipalsManager.updateTitle(aclKey, title)
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            if (title == it.securablePrincipal.title) {
                Result(null, false)
            } else {
                it.securablePrincipal.title = title
                Result(null)
            }
        })
        eventBus.post(OrganizationUpdatedEvent(organizationId, Optional.of(title), Optional.empty()))
    }

    @Timed
    fun updateDescription(organizationId: UUID, description: String) {
        val aclKey = AclKey(organizationId)
        securePrincipalsManager.updateDescription(aclKey, description)
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            if (description == it.securablePrincipal.description) {
                Result(null, false)
            } else {
                it.securablePrincipal.description = description
                Result(null)
            }
        })
        eventBus.post(OrganizationUpdatedEvent(organizationId, Optional.empty(), Optional.of(description)))
    }

    @Timed
    fun getEmailDomains(organizationId: UUID): Set<String> {
        return organizations[organizationId]?.emailDomains ?: setOf()
    }

    fun ensureOrganizationExists(id: UUID) {
        Preconditions.checkState(
                organizations.containsKey(id),
                "Organization [$id] does not exist."
        )
    }

    @Timed
    fun setEmailDomains(organizationId: UUID, emailDomains: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor { organization ->
            organization.emailDomains.clear()
            Result(organization.emailDomains.addAll(emailDomains))
        })

    }

    @Timed
    fun addEmailDomains(organizationId: UUID, emailDomains: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor { organization ->
            val modified = organization.emailDomains.addAll(emailDomains)
            Result(modified, modified)
        })
    }

    @Timed
    fun removeEmailDomains(organizationId: UUID, emailDomains: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor { organization ->
            val modified = organization.emailDomains.removeAll(emailDomains)
            Result(modified, modified)
        })
    }

    @Timed
    fun getMembers(organizationId: UUID): Set<Principal> {
        return securePrincipalsManager.getOrganizationMembers(setOf(organizationId))
                .getValue(organizationId)
                .map { it.principal }
                .toSet()
    }

    @Timed
    fun getMemberCountsForOrganizations(organizationIds: Set<UUID>): Map<UUID, Int> {
        val orgSecurablePrincipals = organizations.getAll(organizationIds).values.map { it.securablePrincipal }.toSet()
        return securePrincipalsManager.bulkGetUnderlyingPrincipals(orgSecurablePrincipals)
                .map { it.key.id to it.value.size }
                .toMap()
    }

    @Timed
    @JvmOverloads
    fun addMembers(
            organizationId: UUID,
            members: Set<Principal>,
            profiles: Map<Principal, Map<String, Set<String>>> = members
                    .associateWith { getAppMetadata(users.getValue(it.id)) }
    ) {
        val nonUserPrincipals = members.filter { it.type != PrincipalType.USER }
        require(nonUserPrincipals.isEmpty()) { "Cannot add non-users principals $nonUserPrincipals to organization $organizationId" }

        val securablePrincipals = members.associateWith { principal -> securePrincipalsManager.lookup(principal) }

        val newMembers = addMembers(AclKey(organizationId), securablePrincipals, profiles)

        if (newMembers.isNotEmpty()) {
            val newMemberSecurablePrincipals = securePrincipalsManager.getSecurablePrincipals(newMembers)
            eventBus.post(
                    MembersAddedToOrganizationEvent(
                            organizationId, SecurablePrincipalList(newMemberSecurablePrincipals.toMutableList())
                    )
            )
        }
    }

    /**
     * Do not invoke this directly as it bypasses checks in [HazelcastOrganizationService.addMembers]
     *
     */
    private fun addMembers(
            orgAclKey: AclKey,
            membersToAdd: Map<Principal, AclKey>,
            profiles: Map<Principal, Map<String, Set<String>>>
    ): Set<Principal> {
        require(orgAclKey.size == 1) { "Organization acl key should only be of length 1" }
        val members = membersToAdd.keys.toSet()
        val organizationId = orgAclKey[0]

        //Always trigger as this won't cause a write to organizations table.
        return grantOrganizationPrincipals(
                organizationId,
                membersToAdd,
                orgAclKey,
                profiles
        )
    }

    private fun grantOrganizationPrincipals(
            organizationId: UUID,
            members: Map<Principal, AclKey>,
            orgAclKey: AclKey = AclKey(organizationId),
            profiles: Map<Principal, Map<String, Set<String>>> = mapOf()
    ): Set<Principal> {
        //Add the organization principal to each user
        val newMemberAclKeys = securePrincipalsManager.addPrincipalToPrincipals(orgAclKey, members.values.toSet())
        val newMembers = members.filter { newMemberAclKeys.contains(it.value) }

        //Grant read on the organization
        val newMemberPermissions = EnumSet.of(Permission.READ)
        authorizations.addPermissions(listOf(
                Acl(AclKey(organizationId), newMembers.keys.map { Ace(it, newMemberPermissions) })
        ))

        /*
         * Grant each new member any of the roles for which they meet the criteria.
         */
        val organization = organizations.getValue(organizationId)
        newMembers.forEach { (member, _) ->
            organization.grants.forEach { (roleId, grants) ->
                grants.forEach { (_, grant) ->
                    val profile = profiles.getOrElse(member) { mapOf() }
                    val granted = when (grant.grantType) {
                        GrantType.Automatic -> true
                        GrantType.Groups -> grant.mappings.intersect(
                                profile.getOrDefault("groups", setOf())
                        ).isNotEmpty()
                        GrantType.Roles -> grant.mappings.intersect(profile.getOrDefault("roles", setOf())).isNotEmpty()
                        GrantType.Attributes -> grant.mappings
                                .intersect(profile.getOrDefault(grant.attribute, setOf()))
                                .isNotEmpty()
                        GrantType.EmailDomain -> grant.mappings.map { it.substring(it.indexOf("@")) }
                                .intersect(profile.getOrDefault("domains", setOf()))
                                .isNotEmpty()
                        //Some grants aren't implemented yet.
                        else -> false
                    }
                    if (granted) {
                        addRoleToPrincipalInOrganization(organizationId, roleId, member)
                    }
                }
            }
        }

        return newMembers.keys
    }

    @Timed
    fun removeMembers(organizationId: UUID, members: Set<Principal>) {
        val users = members.filter { it.type == PrincipalType.USER }
        val securablePrincipals = securePrincipalsManager.getSecurablePrincipals(users)
        val userAclKeys = securePrincipalsManager
                .getSecurablePrincipals(users)
                .map { it.aclKey }
                .toSet()

        removeRolesFromMembers(getRoles(organizationId).map { it.aclKey }, userAclKeys)

        val orgAclKey = AclKey(organizationId)
        removeOrganizationFromMembers(orgAclKey, userAclKeys)
        eventBus.post(
                MembersRemovedFromOrganizationEvent(
                        organizationId,
                        SecurablePrincipalList(securablePrincipals.toMutableList())
                )
        )
    }

    private fun removeRolesFromMembers(roles: Collection<AclKey>, members: Set<AclKey>) {
        securePrincipalsManager.removePrincipalsFromPrincipals(roles.toSet(), members)
    }

    private fun removeOrganizationFromMembers(organization: AclKey, members: Set<AclKey>) {
        securePrincipalsManager.removePrincipalsFromPrincipals(setOf(organization), members)
    }

    @Timed
    fun createRoleIfNotExists(callingUser: Principal, role: Role) {
        val organizationId = role.organizationId
        val orgPrincipal = securePrincipalsManager
                .getSecurablePrincipal(AclKey(organizationId))

        /*
         * We set the organization to be the owner of the principal and grant everyone in the organization read access
         * to the principal. This is done so that anyone in the organization can see the principal and the owners of
         * an organization all have owner on the principal. The principals manager is also responsible for
         * instantiating the role in the postgres materialized views server.
         */
        securePrincipalsManager.createSecurablePrincipalIfNotExists(callingUser, role)

        authorizations.addPermission(role.aclKey, orgPrincipal.principal, EnumSet.of(Permission.READ))

        /*
         * Grant the organization admin role OWNER permissions on the new role,
         * unless the organization admin role is the one being created.
         */
        val orgAdminRole = Principal(
                PrincipalType.ROLE,
                constructOrganizationAdminRolePrincipalId(orgPrincipal)
        )
        if (orgAdminRole != role.principal) {
            authorizations.addPermission(role.aclKey, orgAdminRole, EnumSet.allOf(Permission::class.java))
        }
    }

    @Timed
    fun addRoleToPrincipalInOrganization(organizationId: UUID, roleId: UUID, principal: Principal) {
        val roleAclKey = AclKey(organizationId, roleId)
        val userAclKey = securePrincipalsManager.lookup(principal)

        if (!securePrincipalsManager.principalHasChildPrincipal(userAclKey, roleAclKey)) {
            securePrincipalsManager.addPrincipalToPrincipal(roleAclKey, userAclKey)
        }
    }

    @Timed
    fun getRoles(organizationId: UUID): Set<Role> {
        return securePrincipalsManager.getAllRolesInOrganization(organizationId)
                .map { sp -> sp as Role }
                .toSet()
    }

    @Timed
    fun removeRoleFromUser(roleKey: AclKey, user: Principal) {
        securePrincipalsManager.removePrincipalFromPrincipal(roleKey, securePrincipalsManager.lookup(user))
    }

    @Timed
    fun addAppToOrg(organizationId: UUID, appId: UUID) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            val modified = it.apps.add(appId)
            Result(modified, modified)
        })
    }

    @Timed
    fun removeAppFromOrg(organizationId: UUID, appId: UUID) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            val modified = it.apps.remove(appId)
            Result(modified, modified)
        })

    }

    @Timed
    fun getOrganizationPrincipal(organizationId: UUID): OrganizationPrincipal? {
        val maybeOrganizationPrincipal = securePrincipalsManager
                .getSecurablePrincipals(getOrganizationPredicate(organizationId))
        if (maybeOrganizationPrincipal.isEmpty()) {
            logger.error("Organization id {} has no corresponding securable principal.", organizationId)
            return null
        }
        return Iterables.getOnlyElement(maybeOrganizationPrincipal) as OrganizationPrincipal
    }

    @Timed
    fun setSmsEntitySetInformation(entitySetInformationList: Collection<SmsEntitySetInformation>) {
        phoneNumbers.setPhoneNumber(entitySetInformationList)

        entitySetInformationList.groupBy { it.organizationId }.map { (organizationId, entitySetInfoList) ->
            organizations.submitToKey(
                    organizationId, UpdateOrganizationSmsEntitySetInformationEntryProcessor(entitySetInfoList)
            )
        }.forEach { it.toCompletableFuture().get() }
    }

    @Timed
    fun getDefaultPartitions(organizationId: UUID): List<Int> {
        //TODO: This is mainly a pass through for convenience, but could get messy.
        return partitionManager.getDefaultPartitions(organizationId)
    }

    @Timed
    fun updateRoleGrant(organizationId: UUID, roleId: UUID, grant: Grant) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.grants.getOrPut(roleId) { mutableMapOf() }[grant.grantType] = grant
            Result(null) //TODO: Being lazy not implementing diff as this should rarely be called.
        })
    }

    @Timed
    fun addConnections(organizationId: UUID, connections: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.connections += connections
            Result(null) //TODO: Being lazy not implementing diff as this should rarely be called.
        })

        addUsersMatchingConnections(organizationId, connections)
    }

    @Timed
    fun removeConnections(organizationId: UUID, connections: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.connections -= connections
            Result(null) //TODO: Being lazy not implementing diff as this should rarely be called.
        })
    }

    @Timed
    fun setConnections(organizationId: UUID, connections: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.connections.clear()
            Result(it.connections.addAll(connections), true)
        })

        addUsersMatchingConnections(organizationId, connections)
    }

    private fun executeDatabaseNameUpdate(organizationId: UUID, name: String) {
        organizationDatabases.executeOnKey(organizationId) {
            val value = it.value
            value.name = name
            it.setValue(value)
        }
    }

    @Timed
    fun renameOrganizationDatabase(organizationId: UUID, newDatabaseName: String) {
        PostgresDatabases.assertDatabaseNameIsValid(newDatabaseName)
        val currentDatabaseName = getOrganizationDatabaseName(organizationId)

        try {
            assembler.renameOrganizationDatabase(currentDatabaseName, newDatabaseName)
            executeDatabaseNameUpdate(organizationId, newDatabaseName)
        } catch (e: Exception) {
            throw IllegalStateException(
                    "An error occurred while trying to rename org $organizationId database " +
                            "name to $newDatabaseName", e
            )
        }
    }

    fun getOrganizationsWithoutUserAndWithConnection(connections: Collection<String>, principal: Principal): Set<UUID> {
        return organizations.keySet(
                Predicates.and(
                        Predicates.`in`<UUID, Organization>(CONNECTIONS_INDEX, *connections.toTypedArray()),
                        Predicates.not<UUID, Organization>(
                                Predicates.`in`<UUID, Organization>(
                                        MEMBERS_INDEX,
                                        principal
                                )
                        )
                )
        )
    }

    private fun addUsersMatchingConnections(organizationId: UUID, connections: Set<String>) {
        val usersWithConnections = users.aggregate(UsersWithConnectionsAggregator(connections, mutableSetOf()))
        val profiles = usersWithConnections.associate {
            Principal(PrincipalType.USER, it.id) to getAppMetadata(it)
        }

        addMembers(organizationId, profiles.keys.toSet(), profiles)
    }

    @JvmOverloads
    fun removeMemberFromAllOrganizations(principal: Principal, clearPermissions: Boolean = true) {
        val organizationIds = securePrincipalsManager.getAllPrincipals(securePrincipalsManager.getPrincipal(principal.id))
                .filter { it.principalType == PrincipalType.ORGANIZATION }
                .map { it.id }

        organizationIds.forEach { removeMembers(it, setOf(principal)) }

        logger.info("Removed {} from organizations: {}", principal, organizationIds)
    }

    fun getOrganizationMetadataEntitySetIds(organizationId: UUID): OrganizationMetadataEntitySetIds {
        return organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            Result(it.organizationMetadataEntitySetIds, false)
        }) as OrganizationMetadataEntitySetIds? ?: throw ResourceNotFoundException(
                "Unable able to resolve organization $organizationId"
        )
    }

    fun setOrganizationMetadataEntitySetIds(
            organizationId: UUID,
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds
    ) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.organizationMetadataEntitySetIds = organizationMetadataEntitySetIds
            Result(null, true)
        })
    }

    companion object {

        private val logger = LoggerFactory.getLogger(HazelcastOrganizationService::class.java)

        private fun constructOrganizationAdminRolePrincipalTitle(organization: SecurablePrincipal): String {
            return organization.name + " - ADMIN"
        }

        private fun constructOrganizationAdminRolePrincipalId(organization: SecurablePrincipal): String {
            return organization.id.toString() + "|" + constructOrganizationAdminRolePrincipalTitle(organization)
        }

        private fun createOrganizationAdminRole(organization: SecurablePrincipal, adminRoleAclKey: AclKey): Role {
            val roleId = adminRoleAclKey[1]
            val principalTitle = constructOrganizationAdminRolePrincipalTitle(organization)
            val principalId = constructOrganizationAdminRolePrincipalId(organization)
            val rolePrincipal = Principal(PrincipalType.ROLE, principalId)
            return Role(
                    Optional.of(roleId),
                    organization.id,
                    rolePrincipal,
                    principalTitle,
                    Optional.of("Administrators of this organization")
            )
        }

        private fun getOrganizationPredicate(organizationId: UUID): Predicate<AclKey, SecurablePrincipal> {
            return Predicates.and(
                    Predicates.equal<UUID, Organization>(
                            PrincipalMapstore.PRINCIPAL_TYPE_INDEX,
                            PrincipalType.ORGANIZATION
                    ),
                    Predicates.equal<UUID, Organization>(PrincipalMapstore.ACL_KEY_ROOT_INDEX, organizationId)
            )
        }
    }
}