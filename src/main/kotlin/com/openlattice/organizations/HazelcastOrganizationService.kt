package com.openlattice.organizations

import com.auth0.json.mgmt.users.User
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.Sets
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.assembler.Assembler
import com.openlattice.authorization.*
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.data.storage.partitions.DEFAULT_PARTITION_COUNT
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.notifications.sms.PhoneNumberService
import com.openlattice.notifications.sms.SmsEntitySetInformation
import com.openlattice.organization.OrganizationPrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.events.*
import com.openlattice.organizations.mapstores.CONNECTIONS_INDEX
import com.openlattice.organizations.mapstores.DOMAINS_INDEX
import com.openlattice.organizations.mapstores.MEMBERS_INDEX
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import com.openlattice.organizations.processors.OrganizationReadEntryProcessor
import com.openlattice.organizations.processors.UpdateOrganizationSmsEntitySetInformationEntryProcessor
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.users.getAppMetadata
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Stream
import javax.inject.Inject
import kotlin.streams.asSequence


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
 * Membership in an organization is stored in the membersOf field whcih is accessed via an IMAP. Only principals of type
 * [PrincipalType.USER]. This is mainly because we don't store the principal type field along with the principal id.
 * This may change in the future.
 *
 *
 * While roles may create organizations they cannot be members. That is an organization created by a role will have no members
 * but principals with that role will have the relevant level of access to that role. In addition, roles that create an
 * organization will not inherit the organization role (as they are not members).
 */
class HazelcastOrganizationService(
        hazelcastInstance: HazelcastInstance,
        private val reservations: HazelcastAclKeyReservationService,
        private val authorizations: AuthorizationManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val phoneNumbers: PhoneNumberService,
        private val partitionManager: PartitionManager,
        private val assembler: Assembler
) {
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap( hazelcastInstance )
    private val users = HazelcastMap.USERS.getMap( hazelcastInstance )

    @Inject
    private lateinit var eventBus: EventBus

    val numberOfPartitions: Int
        get() = partitionManager.getPartitionCount()

    fun getOrganization(p: Principal): OrganizationPrincipal {
        return checkNotNull(securePrincipalsManager.getPrincipal(p.id) as OrganizationPrincipal)
    }

    fun maybeGetOrganization(p: Principal): Optional<SecurablePrincipal> {
        return securePrincipalsManager.maybeGetSecurablePrincipal(p)
    }

    private fun initializeOrganizationPrincipals(principal: Principal, organization: Organization) {
        require(
                securePrincipalsManager.createSecurablePrincipalIfNotExists(
                        principal,
                        organization.securablePrincipal
                )
        ) {
            "Unable to create securable principal for organization. This means the organization probably already exists."
        }

        //Create the admin role for the organization and give it ownership of organization.
        val adminRole = createOrganizationAdminRole(organization.securablePrincipal)
        createRoleIfNotExists(principal, adminRole)
        authorizations.addPermission(
                organization.getAclKey(), adminRole.principal, EnumSet.allOf(Permission::class.java)
        )
        addRoleToPrincipalInOrganization(organization.id, adminRole.id, principal)

        //Grant the creator of the organizations
        authorizations.addPermission(organization.getAclKey(), principal, EnumSet.allOf(Permission::class.java))
    }

    fun createOrganization(principal: Principal, organization: Organization) {
        initializeOrganizationPrincipals(principal, organization)
        initializeOrganization(organization)

        /*
         * Roles shouldn't be members of an organizations.
         *
         * Membership is currently defined as your principal having the
         * organization principal.
         *
         * In order to function roles must have READ access on the organization and
         */

        when (principal.type) {
            PrincipalType.USER ->
                //Add the organization principal to the creator marking them as a member of the organization
                addMembers(organization.getAclKey(), ImmutableSet.of(principal), mapOf())
            PrincipalType.ROLE ->
                //For a role we ensure that it has
                logger.debug("Creating an organization with no members, but accessible by {}", principal)
            else -> throw IllegalStateException("Only users and roles can create organizations.")
        }//Fall throught by design


        //We add the user/role that created the organization to the admin role for the organization

        assembler.createOrganization(organization)
        eventBus.post(OrganizationCreatedEvent(organization))
        setSmsEntitySetInformation(organization.smsEntitySetInfo)
    }

    private fun initializeOrganization(organization: Organization) {
        val organizationId = organization.securablePrincipal.id
        organizations.set(organizationId, organization)

        if (organization.partitions.isEmpty()) {
            organization.partitions.addAll(
                    partitionManager.allocateDefaultPartitions(organizationId, DEFAULT_PARTITION_COUNT)
            )
        }


    }

    fun getOrganization(organizationId: UUID): Organization? {
        val org = organizations[organizationId]
        val roles = getRoles(organizationId)

        org?.roles?.addAll(roles)

        return org
    }

    fun getOrganizationApps(organizationId: UUID): Set<UUID> {
        return organizations.executeOnKey(organizationId, OrganizationReadEntryProcessor {
            it.apps
        }) as Set<UUID>
    }

    fun getOrganizations(organizationIds: Stream<UUID>): Iterable<Organization> {
        //TODO: Figure out why copy is here?
        return organizationIds.asSequence().map(this::getOrganization)
                .filterNotNull()
                .map { org ->
                    Organization(
                            org.securablePrincipal,
                            org.emailDomains,
                            org.members,
                            //TODO: If you're an organization you can view its roles.
                            org.roles,
                            org.smsEntitySetInfo,
                            org.partitions,
                            org.apps
                    )
                }.asIterable()
    }

    fun destroyOrganization(organizationId: UUID) {
        // Remove all roles
        val aclKey = AclKey(organizationId)
        authorizations.deletePermissions(aclKey)
        securePrincipalsManager.deleteAllRolesInOrganization(organizationId)
        securePrincipalsManager.deletePrincipal(aclKey)
        organizations.delete(organizationId)
        reservations.release(organizationId)
        assembler.destroyOrganization(organizationId)
        eventBus.post(OrganizationDeletedEvent(organizationId))
    }

    fun updateTitle(organizationId: UUID, title: String) {
        val aclKey = AclKey(organizationId)
        securePrincipalsManager.updateTitle(aclKey, title)
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.securablePrincipal.title = title
            null
        })
        eventBus.post(OrganizationUpdatedEvent(organizationId, Optional.of(title), Optional.empty()))
    }

    fun updateDescription(organizationId: UUID, description: String) {
        val aclKey = AclKey(organizationId)
        securePrincipalsManager.updateDescription(aclKey, description)
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.securablePrincipal.description = description
            null
        })
        eventBus.post(OrganizationUpdatedEvent(organizationId, Optional.empty(), Optional.of(description)))
    }

    fun getEmailDomains(organizationId: UUID): Set<String> {
        return organizations[organizationId]?.emailDomains ?: setOf()
    }

    fun setEmailDomains(organizationId: UUID, emailDomains: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor { organization ->
            organization.emailDomains.clear()
            organization.emailDomains.addAll(emailDomains)
        })

    }

    fun addEmailDomains(organizationId: UUID, emailDomains: Set<String>) {
        organizations.executeOnKey(organizationId,
                OrganizationEntryProcessor { organization ->
                    organization.emailDomains.addAll(emailDomains)
                })
    }

    fun removeEmailDomains(organizationId: UUID, emailDomains: Set<String>) {
        organizations.executeOnKey(organizationId,
                OrganizationEntryProcessor { organization ->
                    organization.emailDomains.removeAll(emailDomains)
                })
    }

    fun getMembers(organizationId: UUID): Set<Principal> {
        return organizations[organizationId]?.members ?: setOf()
    }

    @JvmOverloads
    fun addMembers(
            organizationId: UUID,
            members: Set<Principal>,
            profiles: Map<Principal, Map<String, Set<String>>> = members
                    .associateWith { getAppMetadata(users.getValue(it.id)) }
    ) {
        val newMembers = addMembers(AclKey(organizationId), members, profiles)

        if (newMembers.isNotEmpty()) {
            val securablePrincipals = securePrincipalsManager.getSecurablePrincipals(newMembers)
            eventBus.post(
                    MembersAddedToOrganizationEvent(organizationId, SecurablePrincipalList(securablePrincipals.toMutableList()))
            )
        }
    }

    private fun addMembers(
            orgAclKey: AclKey,
            members: Set<Principal>,
            profiles: Map<Principal, Map<String, Set<String>>>
    ): Set<Principal> {
        require(orgAclKey.size == 1) { "Organization acl key should only be of length 1" }

        val nonUserPrincipals = members.filter { it.type != PrincipalType.USER }
        require(nonUserPrincipals.isEmpty()) { "Cannot add non-users principals $nonUserPrincipals to organization $orgAclKey" }
        val organizationId = orgAclKey[0]


        val newMembers: Set<Principal> = organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            val newMembers = members.filter { member -> !it.members.contains(member) }.toSet()
            it.members.addAll(newMembers)
            return@OrganizationEntryProcessor newMembers
        }) as Set<Principal>

        if (newMembers.isNotEmpty()) {
            grantOrganizationPrincipals(organizationId, newMembers, orgAclKey, profiles)
        }

        return newMembers
    }

    private fun grantOrganizationPrincipals(
            organizationId: UUID,
            members: Set<Principal>,
            orgAclKey: AclKey = AclKey(organizationId),
            profiles: Map<Principal, Map<String, Set<String>>> = mapOf()
    ) {
        /*
         * Grant each member any of the roles, for which they meet the crieria.
         */
        val organization = organizations.getValue(organizationId)
        members.forEach { member ->
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
        //Add the organization principal to each user
        members.map { principal ->
            //Grant read on the organization
            authorizations.addPermission(orgAclKey, principal, EnumSet.of(Permission.READ))
            //Assign organization principal.
            securePrincipalsManager.lookup(principal)
        }.forEach { target -> securePrincipalsManager.addPrincipalToPrincipal(orgAclKey, target) }

    }

    fun removeMembers(organizationId: UUID, members: Set<Principal>) {
        val users = members.filter { it.type == PrincipalType.USER }
        val securablePrincipals = securePrincipalsManager.getSecurablePrincipals(users)
        val userAclKeys = securePrincipalsManager
                .getSecurablePrincipals(users)
                .map { it.aclKey }
                .toSet()

        removeRolesFromMembers(getRoles(organizationId).map { it.aclKey }, userAclKeys)
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.members.removeAll(members)
        })

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
        securePrincipalsManager.removePrincipalsFromPrincipals(roles, members)
    }

    private fun removeOrganizationFromMembers(organization: AclKey, members: Set<AclKey>) {
        securePrincipalsManager.removePrincipalsFromPrincipals(listOf(organization), members)
    }

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
                constructOrganizationAdminRolePrincipalId(orgPrincipal))
        if (orgAdminRole != role.principal) {
            authorizations.addPermission(role.aclKey, orgAdminRole, EnumSet.allOf(Permission::class.java))
        }
    }

    fun addRoleToPrincipalInOrganization(organizationId: UUID, roleId: UUID, principal: Principal) {
        val roleKey = AclKey(organizationId, roleId)
        val userPrincipal = securePrincipalsManager.lookup(principal)

        if (!securePrincipalsManager.principalHasChildPrincipal(userPrincipal, roleKey)) {
            securePrincipalsManager.addPrincipalToPrincipal(roleKey, userPrincipal)
        }
    }

    fun getRoles(organizationId: UUID): Set<Role> {
        return securePrincipalsManager.getAllRolesInOrganization(organizationId)
                .map { sp -> sp as Role }
                .toSet()
    }

    fun removeRoleFromUser(roleKey: AclKey, user: Principal) {
        securePrincipalsManager.removePrincipalFromPrincipal(roleKey, securePrincipalsManager.lookup(user))
    }

    fun addAppToOrg(organizationId: UUID, appId: UUID) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.apps.add(appId)
        })
    }

    fun removeAppFromOrg(organizationId: UUID, appId: UUID) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.apps.remove(appId)
        })

    }

    private fun fixOrganizations() {
        checkNotNull(AuthorizationInitializationTask.GLOBAL_ADMIN_ROLE.principal)
        logger.info("Fixing organizations.")
        for (organization in securePrincipalsManager
                .getSecurablePrincipals(PrincipalType.ORGANIZATION)) {
            authorizations.setSecurableObjectType(organization.aclKey, SecurableObjectType.Organization)
            authorizations.addPermission(
                    organization.aclKey,
                    AuthorizationInitializationTask.GLOBAL_ADMIN_ROLE.principal,
                    EnumSet.allOf(Permission::class.java)
            )

            logger.info("Setting titles, descriptions, and autoApproved e-mails domains if not present.")
            if (!organizations.containsKey(organization.id)) {
                organizations.set(
                        organization.id,
                        Organization(
                                organization as OrganizationPrincipal,
                                mutableSetOf(),
                                mutableSetOf(),
                                mutableSetOf(),
                                mutableSetOf(),
                                mutableListOf()
                        )
                )
            }

            logger.info("Synchronizing roles")
            val roles = securePrincipalsManager.getAllRolesInOrganization(organization.id)
            //Grant the organization principal read permission on each principal
            for (role in roles) {
                authorizations.setSecurableObjectType(role.aclKey, SecurableObjectType.Role)
                authorizations.addPermission(role.aclKey, organization.principal, EnumSet.of(Permission.READ))
            }

            logger.info("Synchronizing members")
            val principals = PrincipalSet.wrap(
                    HashSet(
                            securePrincipalsManager
                                    .getAllUsersWithPrincipal(organization.aclKey)
                    )
            )
            //Add all users who have the organization role to the organizaton.
            addMembers(organization.id, principals)

            /*
             * This is a one time thing so that admins at this point in time have access to and can fix organizations.
             *
             * For simplicity we are going to add all admin users into all organizations. We will have to manually clean
             * this up afterwards.
             */

            logger.info("Synchronizing admins.")
            val adminPrincipals = PrincipalSet.wrap(
                    HashSet(
                            securePrincipalsManager.getAllUsersWithPrincipal(
                                    securePrincipalsManager.lookup(
                                            AuthorizationInitializationTask.GLOBAL_ADMIN_ROLE.principal
                                    )
                            )
                    )
            )

            addMembers(organization.id, adminPrincipals)
            adminPrincipals.forEach { admin ->
                authorizations
                        .addPermission(organization.aclKey, admin, EnumSet.allOf(Permission::class.java))
            }

        }
    }

    fun getOrganizationPrincipal(organizationId: UUID): OrganizationPrincipal? {
        val maybeOrganizationPrincipal = securePrincipalsManager
                .getSecurablePrincipals(getOrganizationPredicate(organizationId))
        if (maybeOrganizationPrincipal.isEmpty()) {
            logger.error("Organization id {} has no corresponding securable principal.", organizationId)
            return null
        }
        return Iterables.getOnlyElement(maybeOrganizationPrincipal) as OrganizationPrincipal
    }

    fun setSmsEntitySetInformation(entitySetInformationList: Collection<SmsEntitySetInformation>) {
        phoneNumbers.setPhoneNumber(entitySetInformationList)

        entitySetInformationList.groupBy { it.organizationId }.map { (organizationId, entitySetInfoList) ->
            organizations.submitToKey(organizationId, UpdateOrganizationSmsEntitySetInformationEntryProcessor(entitySetInfoList))
        }.forEach { it.get() }
    }

    fun getDefaultPartitions(organizationId: UUID): List<Int> {
        //TODO: This is mainly a pass through for convenience, but could get messy.
        return partitionManager.getDefaultPartitions(organizationId)
    }

    fun allocateDefaultPartitions(partitionCount: Int): List<Int> {
        return partitionManager.allocateDefaultPartitions(partitionCount)
    }

    fun removeUser(principal: Principal) {
        organizations.executeOnEntries(OrganizationEntryProcessor { it.members.remove(principal) })
    }

    fun updateRoleGrant(organizationId: UUID, roleId: UUID, grant: Grant) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.grants.getOrPut(roleId) { mutableMapOf() }[grant.grantType] = grant
            null
        })
    }

    fun addConnections(organizationId: UUID, connections: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.connections += connections
            null
        })
    }

    fun removeConnections(organizationId: UUID, connections: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.connections -= connections
            null
        })
    }

    fun setConnections(organizationId: UUID, connections: Set<String>) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.connections.clear()
            it.connections.addAll(connections)
        })
    }

    fun getOrganizationsWithConnection(connection: String): Set<UUID> {
        return organizations.keySet(Predicates.equal(CONNECTIONS_INDEX, connection))
    }

    fun getOrganizationsWithoutUserAndWithConnection(connections: Collection<String>, principal: Principal): Set<UUID> {
        return organizations.keySet(
                Predicates.and(
                        Predicates.`in`(CONNECTIONS_INDEX, *connections.toTypedArray()),
                        Predicates.not(Predicates.`in`(MEMBERS_INDEX, principal) )
                )
        )
    }

    fun getOrganizationsWithoutUserAndWithConnectionsAndDomains(
            principal: Principal,
            connections: Collection<String>,
            emailDomain: String
    ): Set<UUID> {
        return organizations.keySet(
                Predicates.and(
                        Predicates.`in`(CONNECTIONS_INDEX, *connections.toTypedArray()),
                        Predicates.`in`(DOMAINS_INDEX, emailDomain),
                        Predicates.not(Predicates.`in`(MEMBERS_INDEX, principal) )
                )
        )
    }


    companion object {

        private val logger = LoggerFactory.getLogger(HazelcastOrganizationService::class.java)

        private fun constructOrganizationAdminRolePrincipalTitle(organization: SecurablePrincipal): String {
            return organization.name + " - ADMIN"
        }

        private fun constructOrganizationAdminRolePrincipalId(organization: SecurablePrincipal): String {
            return organization.id.toString() + "|" + constructOrganizationAdminRolePrincipalTitle(organization)
        }

        private fun createOrganizationAdminRole(organization: SecurablePrincipal): Role {
            val principalTitle = constructOrganizationAdminRolePrincipalTitle(organization)
            val principalId = constructOrganizationAdminRolePrincipalId(organization)
            val rolePrincipal = Principal(PrincipalType.ROLE, principalId)
            return Role(
                    Optional.empty(),
                    organization.id,
                    rolePrincipal,
                    principalTitle,
                    Optional.of("Administrators of this organization")
            )
        }

        private fun getOrganizationPredicate(organizationId: UUID): Predicate<*, *> {
            return Predicates.and(
                    Predicates.equal("principalType", PrincipalType.ORGANIZATION),
                    Predicates.equal("aclKey[0]", organizationId)
            )
        }
    }
}