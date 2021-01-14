package com.openlattice.organizations.controllers

import com.auth0.json.mgmt.users.User
import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Sets
import com.openlattice.apps.AppTypeSetting
import com.openlattice.apps.services.AppService
import com.openlattice.assembler.Assembler
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.authorization.util.getLastAclKeySafely
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.organization.*
import com.openlattice.organization.OrganizationsApi.Companion.CONTROLLER
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.*
import com.openlattice.organizations.roles.SecurePrincipalsManager
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang3.NotImplementedException
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.stream.Collectors
import java.util.stream.StreamSupport
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
        justification = "Allowing redundant kotlin null check on lateinit variables")
class OrganizationsController : AuthorizingComponent, OrganizationsApi {
    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Inject
    private lateinit var organizations: HazelcastOrganizationService

    @Inject
    private lateinit var assembler: Assembler

    @Inject
    private lateinit var securableObjectTypes: SecurableObjectResolveTypeService

    @Inject
    private lateinit var principalService: SecurePrincipalsManager

    @Inject
    private lateinit var entitySetManager: EntitySetManager

    @Inject
    private lateinit var edms: ExternalDatabaseManagementService

    @Inject
    private lateinit var appService: AppService

    @Inject
    private lateinit var organizationMetadataEntitySetsService: OrganizationMetadataEntitySetsService

    @Inject
    private lateinit var externalDatabaseManagementService: ExternalDatabaseManagementService

    @Inject
    private lateinit var edmService: EdmManager

    @Timed
    @GetMapping(value = ["", "/"], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getOrganizations(): Iterable<Organization> {
        val authorizedRoles: Set<AclKey> = getAccessibleObjects(SecurableObjectType.Role, EnumSet.of(Permission.READ))
                .filter { obj: AclKey -> Objects.nonNull(obj) }.collect(Collectors.toSet())
        val orgs = organizations.getOrganizations(
                getAccessibleObjects(SecurableObjectType.Organization, EnumSet.of(Permission.READ))
                        .parallel()
                        .filter { obj: AclKey -> Objects.nonNull(obj) }
                        .map { aclKeys: AclKey -> getLastAclKeySafely(aclKeys) },
                true
        )
        return StreamSupport.stream(orgs.spliterator(), false).peek { (_, _, _, _, roles) ->
            roles.removeIf { role: Role ->
                !authorizedRoles.contains(
                        role.aclKey
                )
            }
        }.collect(Collectors.toList())
    }

    @Timed
    @GetMapping(value = [OrganizationsApi.METADATA], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getMetadataOfOrganizations(): Iterable<OrganizationPrincipal> {
        val authorizedOrganizationIds: Set<UUID> = getAccessibleObjects(
                SecurableObjectType.Organization,
                EnumSet.of(Permission.READ)
        ).map { ak: AclKey -> ak[0] }.collect(Collectors.toSet<UUID>())
        return organizations.getOrganizationPrincipals(authorizedOrganizationIds)
    }

    @Timed
    @PostMapping(value = ["", "/"], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun createOrganizationIfNotExists(@RequestBody organization: Organization): UUID {
        Preconditions.checkArgument(
                organization.connections.isEmpty() || isAdmin(),
                "Must be admin to specify auto-enrollments"
        )
        organizations.createOrganization(Principals.getCurrentUser(), organization)
        securableObjectTypes.createSecurableObjectType(
                AclKey(organization.id),
                SecurableObjectType.Organization
        )
        return organization.id
    }

    @Timed
    @GetMapping(value = [OrganizationsApi.ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getOrganization(@PathVariable(OrganizationsApi.ID) organizationId: UUID): Organization? {
        ensureRead(organizationId)
        //TODO: Re-visit roles within an organization being defined as roles which have read on that organization.
        val org = organizations.getOrganization(organizationId)
        if (org != null) {
            val authorizedRoleAclKeys = getAuthorizedRoleAclKeys(org.roles)
            org.roles.removeIf { role: Role ->
                !authorizedRoleAclKeys.contains(
                        role.aclKey
                )
            }
        }
        return org
    }

    @Timed
    @DeleteMapping(value = [OrganizationsApi.ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    @ResponseStatus(
            HttpStatus.OK
    )
    override fun destroyOrganization(@PathVariable(OrganizationsApi.ID) organizationId: UUID): Void? {
        ensureOwner(organizationId)
        ensureObjectCanBeDeleted(organizationId)
        organizations.ensureOrganizationExists(organizationId)
        organizations.destroyOrganization(organizationId)
        edms.deleteOrganizationExternalDatabase(organizationId)
        return null
    }

    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.SET_ID_PATH + OrganizationsApi.TRANSPORT],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    @ResponseStatus(
            HttpStatus.OK
    )
    override fun transportEntitySet(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.SET_ID) entitySetId: UUID
    ): Void? {
        organizations.ensureOrganizationExists(organizationId)
        ensureRead(organizationId)
        ensureTransportAccess(AclKey(entitySetId))
        edms.transportEntitySet(organizationId, entitySetId)
        return null
    }

    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.SET_ID_PATH + OrganizationsApi.DESTROY],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    @ResponseStatus(
            HttpStatus.OK
    )
    override fun destroyTransportedEntitySet(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.SET_ID) entitySetId: UUID
    ): Void? {
        organizations.ensureOrganizationExists(organizationId)
        ensureRead(organizationId)
        ensureTransportAccess(AclKey(entitySetId))
        edms.destroyTransportedEntitySet(entitySetId)
        return null
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.INTEGRATION],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getOrganizationIntegrationAccount(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID
    ): OrganizationIntegrationAccount {
        ensureOwner(organizationId)
        return assembler.getOrganizationIntegrationAccount(organizationId)
    }

    @Timed
    @PatchMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.INTEGRATION],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun rollOrganizationIntegrationAccount(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID
    ): OrganizationIntegrationAccount {
        ensureOwner(organizationId)
        val account = assembler.rollIntegrationAccount(AclKey(organizationId), PrincipalType.ORGANIZATION)
        return OrganizationIntegrationAccount(account.username, account.credential)
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.ENTITY_SETS],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getOrganizationEntitySets(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        ensureRead(organizationId)
        return getOrganizationEntitySets(organizationId, EnumSet.allOf(OrganizationEntitySetFlag::class.java))
    }

    @Timed
    @PostMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.ENTITY_SETS],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getOrganizationEntitySets(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody flagFilter: EnumSet<OrganizationEntitySetFlag>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        ensureRead(organizationId)
        val orgPrincipal = checkNotNull(
                organizations.getOrganizationPrincipal(organizationId)
        ) { "Invalid organization specified." }
        val internal = entitySetManager.getEntitySetsForOrganization(organizationId)
        val external = authorizations.getAuthorizedObjectsOfType(
                orgPrincipal.principal,
                SecurableObjectType.EntitySet,
                EnumSet.of(Permission.MATERIALIZE)
        )
        val materialized = assembler.getMaterializedEntitySetsInOrganization(organizationId)
        val entitySets: MutableMap<UUID, MutableSet<OrganizationEntitySetFlag>> = HashMap(
                2 * internal.size
        )
        if (flagFilter.contains(OrganizationEntitySetFlag.INTERNAL)) {
            internal.forEach { entitySetId ->
                entitySets
                        .merge(
                                entitySetId, EnumSet.of(OrganizationEntitySetFlag.INTERNAL)
                        ) { lhs: MutableSet<OrganizationEntitySetFlag>, rhs: MutableSet<OrganizationEntitySetFlag> ->
                            lhs.addAll(rhs)
                            lhs
                        }
            }
        }
        if (flagFilter.contains(OrganizationEntitySetFlag.EXTERNAL)) {
            external.map { aclKey -> aclKey.get(0) }.forEach { entitySetId ->
                entitySets
                        .merge(
                                entitySetId, EnumSet.of(OrganizationEntitySetFlag.EXTERNAL)
                        ) { lhs: MutableSet<OrganizationEntitySetFlag>, rhs: MutableSet<OrganizationEntitySetFlag> ->
                            lhs.addAll(rhs)
                            lhs
                        }
            }
        }
        if (flagFilter.contains(OrganizationEntitySetFlag.MATERIALIZED)
                || flagFilter.contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
                || flagFilter.contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)) {
            materialized.forEach { (entitySetId, flags) ->
                if (flagFilter.contains(OrganizationEntitySetFlag.MATERIALIZED)) {
                    entitySets.merge(
                            entitySetId, EnumSet.of(OrganizationEntitySetFlag.MATERIALIZED)
                    ) { lhs: MutableSet<OrganizationEntitySetFlag>, rhs: MutableSet<OrganizationEntitySetFlag> ->
                        lhs.addAll(rhs)
                        lhs
                    }
                }
                if (flagFilter.contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
                        && flags.contains(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)) {
                    entitySets.merge(
                            entitySetId, EnumSet.of(OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)
                    ) { lhs: MutableSet<OrganizationEntitySetFlag>, rhs: MutableSet<OrganizationEntitySetFlag> ->
                        lhs.addAll(rhs)
                        lhs
                    }
                }
                if (flagFilter.contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
                        && flags.contains(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)) {
                    entitySets.merge(
                            entitySetId, EnumSet.of(OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)
                    ) { lhs: MutableSet<OrganizationEntitySetFlag>, rhs: MutableSet<OrganizationEntitySetFlag> ->
                        lhs.addAll(rhs)
                        lhs
                    }
                }
            }
        }
        return entitySets
    }

    @Timed
    @PostMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.ENTITY_SETS + OrganizationsApi.ASSEMBLE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun assembleEntitySets(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody refreshRatesOfEntitySets: Map<UUID, Int>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        throw NotImplementedException("DBT will fill this in.")
    }

    @Timed
    @PostMapping(
            OrganizationsApi.ID_PATH + OrganizationsApi.SET_ID_PATH + OrganizationsApi.SYNCHRONIZE
    )
    override fun synchronizeEdmChanges(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.SET_ID) entitySetId: UUID
    ): Void? {
        throw NotImplementedException("DBT will fill this in.")
    }

    @Timed
    @PostMapping(
            OrganizationsApi.ID_PATH + OrganizationsApi.SET_ID_PATH + OrganizationsApi.REFRESH
    )
    override fun refreshDataChanges(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.SET_ID) entitySetId: UUID
    ): Void? {
        // the person requesting refresh should be the owner of the organization
        ensureOwner(organizationId)
        throw NotImplementedException("DBT will fill this in.")
    }

    @PutMapping(OrganizationsApi.ID_PATH + OrganizationsApi.SET_ID_PATH + OrganizationsApi.REFRESH_RATE)
    override fun updateRefreshRate(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.SET_ID) entitySetId: UUID,
            @RequestBody refreshRate: Int
    ): Void? {
        ensureOwner(organizationId)
        val refreshRateInMilliSecs = getRefreshRateMillisFromMins(refreshRate)
        assembler.updateRefreshRate(organizationId, entitySetId, refreshRateInMilliSecs)
        return null
    }

    @DeleteMapping(OrganizationsApi.ID_PATH + OrganizationsApi.SET_ID_PATH + OrganizationsApi.REFRESH_RATE)
    override fun deleteRefreshRate(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.SET_ID) entitySetId: UUID
    ): Void? {
        ensureOwner(organizationId)
        assembler.updateRefreshRate(organizationId, entitySetId, null)
        return null
    }

    private fun getRefreshRateMillisFromMins(refreshRateInMins: Int): Long {
        require(refreshRateInMins >= 1) { "Minimum refresh rate is 1 minute." }

        // convert mins to millisecs
        return refreshRateInMins.toLong() * 3600L
    }

    @Timed
    @PutMapping(value = [OrganizationsApi.ID_PATH + OrganizationsApi.TITLE], consumes = [MediaType.TEXT_PLAIN_VALUE])
    @ResponseStatus(
            HttpStatus.OK
    )
    override fun updateTitle(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody title: String
    ): Void? {
        ensureOwner(organizationId)
        organizations.updateTitle(organizationId, title)
        return null
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.DESCRIPTION], consumes = [MediaType.TEXT_PLAIN_VALUE]
    )
    @ResponseStatus(
            HttpStatus.OK
    )
    override fun updateDescription(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody description: String
    ): Void? {
        ensureOwner(organizationId)
        organizations.updateDescription(organizationId, description)
        return null
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.EMAIL_DOMAINS],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAutoApprovedEmailDomains(@PathVariable(OrganizationsApi.ID) organizationId: UUID): Set<String> {
        ensureOwner(organizationId)
        return organizations.getEmailDomains(organizationId)
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.EMAIL_DOMAINS],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    @ResponseStatus(
            HttpStatus.OK
    )
    override fun setAutoApprovedEmailDomain(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody emailDomains: Set<String>
    ): Void? {
        ensureAdminAccess()
        organizations.setEmailDomains(organizationId, emailDomains)
        return null
    }

    @Timed
    @PostMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.EMAIL_DOMAINS],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun addEmailDomains(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody emailDomains: Set<String>
    ): Void? {
        ensureAdminAccess()
        organizations.addEmailDomains(organizationId, emailDomains)
        return null
    }

    @Timed
    @DeleteMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.EMAIL_DOMAINS],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun removeEmailDomains(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody emailDomains: Set<String>
    ): Void? {
        ensureAdminAccess()
        organizations.removeEmailDomains(organizationId, emailDomains)
        return null
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.EMAIL_DOMAINS + OrganizationsApi.EMAIL_DOMAIN_PATH]
    )
    override fun addEmailDomain(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.EMAIL_DOMAIN) emailDomain: String
    ): Void? {
        ensureAdminAccess()
        organizations.addEmailDomains(organizationId, ImmutableSet.of(emailDomain))
        return null
    }

    @Timed
    @DeleteMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.EMAIL_DOMAINS + OrganizationsApi.EMAIL_DOMAIN_PATH]
    )
    override fun removeEmailDomain(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.EMAIL_DOMAIN) emailDomain: String
    ): Void? {
        ensureAdminAccess()
        organizations.removeEmailDomains(organizationId, ImmutableSet.of(emailDomain))
        return null
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.PRINCIPALS + OrganizationsApi.MEMBERS + OrganizationsApi.COUNT],
            consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getMemberCountForOrganizations(organizationIds: Set<UUID>): Map<UUID, Int> {
        val readPermissions = EnumSet.of(Permission.READ)
        accessCheck(
                organizationIds.stream().collect(
                        Collectors.toMap(
                                { uuids: UUID -> AclKey(uuids) },
                                { readPermissions })
                )
        )
        return organizations.getMemberCountsForOrganizations(organizationIds)
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.COUNT],
            consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getRoleCountForOrganizations(organizationIds: Set<UUID>): Map<UUID, Int> {
        val readPermissions = EnumSet.of(Permission.READ)
        accessCheck(
                organizationIds.stream().collect(
                        Collectors.toMap(
                                { uuids: UUID -> AclKey(uuids) },
                                { readPermissions })
                )
        )
        return organizations.getRoleCountsForOrganizations(organizationIds)
    }

    @Timed
    @GetMapping(value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.MEMBERS])
    override fun getMembers(@PathVariable(OrganizationsApi.ID) organizationId: UUID): Iterable<OrganizationMember> {
        ensureRead(organizationId)
        val members = organizations.getMembers(organizationId)
        val securablePrincipals = principalService.getSecurablePrincipals(members)
        return Iterable {
            securablePrincipals
                    .stream()
                    .map { sp: SecurablePrincipal ->
                        OrganizationMember(
                                sp,
                                principalService.getUser(sp.name),
                                principalService.getAllPrincipals(sp)
                        )
                    }.iterator()
        }
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.MEMBERS + OrganizationsApi.USER_ID_PATH]
    )
    override fun addMember(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.USER_ID) userId: String
    ): Void? {
        ensureOwnerAccess(AclKey(organizationId))
        organizations.addMembers(
                organizationId,
                ImmutableSet.of(Principal(PrincipalType.USER, userId))
        )
        return null
    }

    @Timed
    @DeleteMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.MEMBERS + OrganizationsApi.USER_ID_PATH]
    )
    override fun removeMember(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.USER_ID) userId: String
    ): Void? {
        ensureOwnerAccess(AclKey(organizationId))
        organizations.removeMembers(
                organizationId, ImmutableSet.of(
                Principal(PrincipalType.USER, userId)
        )
        )
        edms.revokeAllPrivilegesFromMember(organizationId, userId)
        return null
    }

    @Timed
    @PostMapping(value = [OrganizationsApi.ROLES], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun createRole(@RequestBody role: Role): UUID {
        ensureOwner(role.organizationId)
        //We only create the role, but do not necessarily assign it to ourselves.
        organizations.createRoleIfNotExists(Principals.getCurrentUser(), role)
        return role.id
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getRoles(@PathVariable(OrganizationsApi.ID) organizationId: UUID): Set<Role> {
        ensureRead(organizationId)
        val roles = organizations.getRoles(organizationId)
        val authorizedRoleAclKeys = getAuthorizedRoleAclKeys(roles)
        return Sets.filter(roles) { role -> role != null && authorizedRoleAclKeys.contains(role.aclKey) }
    }

    private fun getAuthorizedRoleAclKeys(roles: Set<Role>): Set<AclKey> {
        return authorizations
                .accessChecksForPrincipals(roles.stream()
                        .map { role: Role ->
                            AccessCheck(
                                    role.aclKey, EnumSet.of(Permission.READ)
                            )
                        }
                        .collect(Collectors.toSet()), Principals.getCurrentPrincipals())
                .filter { authorization: Authorization ->
                    authorization.permissions.getOrDefault(Permission.READ, false)
                }
                .map { obj: Authorization -> obj.aclKey }.collect(Collectors.toSet())
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getRole(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID
    ): Role {
        val aclKey = AclKey(organizationId, roleId)
        return if (isAuthorized(Permission.READ).test(aclKey)) {
            principalService.getRole(organizationId, roleId)
        } else {
            throw ForbiddenException("Unable to find role: $aclKey")
        }
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH + OrganizationsApi.TITLE],
            consumes = [MediaType.TEXT_PLAIN_VALUE]
    )
    override fun updateRoleTitle(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID,
            @RequestBody title: String
    ): Void? {
        ensureRoleAdminAccess(organizationId, roleId)
        //TODO: Do this in a less crappy way
        principalService.updateTitle(AclKey(organizationId, roleId), title)
        return null
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH + OrganizationsApi.DESCRIPTION],
            consumes = [MediaType.TEXT_PLAIN_VALUE]
    )
    override fun updateRoleDescription(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID,
            @RequestBody description: String
    ): Void? {
        ensureRoleAdminAccess(organizationId, roleId)
        principalService.updateDescription(AclKey(organizationId, roleId), description)
        return null
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH + OrganizationsApi.GRANT],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateRoleGrant(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID,
            @RequestBody grant: Grant
    ): Void? {
        ensureRoleAdminAccess(organizationId, roleId)
        organizations.updateRoleGrant(organizationId, roleId, grant)
        return null
    }

    @Timed
    @DeleteMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH]
    )
    override fun deleteRole(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID
    ): Void? {
        ensureRoleAdminAccess(organizationId, roleId)
        ensureObjectCanBeDeleted(roleId)
        ensureRoleIsNotAdminRole(organizationId, roleId)
        ensureRoleNotUsedByApp(organizationId, roleId)
        principalService.deletePrincipal(AclKey(organizationId, roleId))
        return null
    }

    @Timed
    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH + OrganizationsApi.MEMBERS],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAllUsersOfRole(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID
    ): Iterable<User> {
        ensureRead(organizationId)
        return principalService.getAllUserProfilesWithPrincipal(AclKey(organizationId, roleId))
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH + OrganizationsApi.MEMBERS + OrganizationsApi.USER_ID_PATH]
    )
    override fun addRoleToUser(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID,
            @PathVariable(OrganizationsApi.USER_ID) userId: String
    ): Void? {
        ensureOwnerAccess(AclKey(organizationId, roleId))
        organizations.addRoleToPrincipalInOrganization(
                organizationId,
                roleId,
                Principal(PrincipalType.USER, userId)
        )
        return null
    }

    @Timed
    @DeleteMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.PRINCIPALS + OrganizationsApi.ROLES + OrganizationsApi.ROLE_ID_PATH + OrganizationsApi.MEMBERS + OrganizationsApi.USER_ID_PATH]
    )
    override fun removeRoleFromUser(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.ROLE_ID) roleId: UUID,
            @PathVariable(OrganizationsApi.USER_ID) userId: String
    ): Void? {
        ensureOwnerAccess(AclKey(organizationId, roleId))
        organizations.removeRoleFromUser(
                AclKey(organizationId, roleId),
                Principal(PrincipalType.USER, userId)
        )
        return null
    }

    @Timed
    @PostMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.CONNECTIONS],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun addConnections(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID, @RequestBody connections: Set<String>
    ): Void? {
        ensureAdminAccess()
        organizations.addConnections(organizationId, connections)
        return null
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.CONNECTIONS],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun setConnections(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID, @RequestBody connections: Set<String>
    ): Void? {
        ensureAdminAccess()
        organizations.setConnections(organizationId, connections)
        return null
    }

    @Timed
    @PutMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.METADATA_ENTITY_SET_IDS],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun setMetadataEntitySetIds(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody entitySetIds: OrganizationMetadataEntitySetIds
    ): Void? {
        ensureOwner(organizationId)
        organizations.setOrganizationMetadataEntitySetIds(organizationId, entitySetIds)
        return null
    }

    @Timed
    @PostMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.METADATA],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importMetadata(@PathVariable(OrganizationsApi.ID) organizationId: UUID): Void? {
        ensureAdminAccess()
        ensureOwner(organizationId)
        val adminRoleAclKey = organizations.getAdminRoleAclKey(organizationId)
        organizationMetadataEntitySetsService.initializeOrganizationMetadataEntitySets(
                principalService
                        .getRole(adminRoleAclKey[0], adminRoleAclKey[1])
        )

        val orgTables = externalDatabaseManagementService.getExternalDatabaseTables(organizationId)
        val tableCols = externalDatabaseManagementService.getColumnsForTables(orgTables.keys)

        orgTables.values.groupBy { it.organizationId }.forEach { (orgId, tables) ->
            val cols = tables.associate { it.id to (tableCols[it.id]?.values ?: listOf()) }

            organizationMetadataEntitySetsService.addDatasetsAndColumns(organizationId, tables, cols)
        }

        entitySetManager
                .getEntitySetsForOrganization(organizationId)
                .forEach { e: UUID ->
                    val entitySet = checkNotNull(entitySetManager.getEntitySet(e)) {
                        "Entity set was null when importing metadata"
                    }
                    val propertyTypes = edmService.getPropertyTypesOfEntityType(entitySet.entityTypeId)
                    organizationMetadataEntitySetsService.addDatasetsAndColumns(
                            ImmutableList.of(entitySet),
                            ImmutableMap.of(entitySet.id, propertyTypes.values)
                    )
                }
        return null
    }

    @Timed
    @DeleteMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.CONNECTIONS],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun removeConnections(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID, @RequestBody connections: Set<String>
    ): Void? {
        organizations.removeConnections(organizationId, connections)
        return null
    }

    @Timed
    @PostMapping(value = [OrganizationsApi.PROMOTE + OrganizationsApi.ID_PATH], consumes = [MediaType.TEXT_PLAIN_VALUE])
    override fun promoteStagingTable(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID, @RequestBody tableName: String
    ): Void? {
        ensureOwner(organizationId)
        edms.promoteStagingTable(organizationId, tableName)
        return null
    }

    @GetMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.DATABASE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getOrganizationDatabaseName(@PathVariable(OrganizationsApi.ID) organizationId: UUID): String {
        ensureRead(organizationId)
        return organizations.getOrganizationDatabaseName(organizationId)
    }

    @PatchMapping(
            value = [OrganizationsApi.ID_PATH + OrganizationsApi.DATABASE],
            consumes = [MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_FORM_URLENCODED_VALUE]
    )
    override fun renameOrganizationDatabase(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody newDatabaseName: String
    ): Void? {
        ensureOwner(organizationId)
        organizations.renameOrganizationDatabase(organizationId, newDatabaseName)
        return null
    }

    private fun ensureRoleAdminAccess(organizationId: UUID, roleId: UUID) {
        ensureOwner(organizationId)
        val aclKey = AclKey(organizationId, roleId)
        accessCheck(aclKey, EnumSet.of(Permission.OWNER))
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }

    private fun ensureOwner(organizationId: UUID): AclKey {
        val aclKey = AclKey(organizationId)
        accessCheck(aclKey, EnumSet.of(Permission.OWNER))
        return aclKey
    }

    private fun ensureRead(organizationId: UUID) {
        ensureReadAccess(AclKey(organizationId))
    }

    private fun ensureRoleNotUsedByApp(organizationId: UUID, roleId: UUID) {
        val aclKey = AclKey(organizationId, roleId)
        appService.getOrganizationAppsByAppId(
                organizationId
        ).forEach { (appId: UUID, appTypeSetting: AppTypeSetting) ->
            appTypeSetting.roles.forEach { (appRoleId: UUID, roleAclKey: AclKey) ->
                require(roleAclKey != aclKey) {
                    ("Role " + aclKey.toString()
                            + " cannot be deleted because it is tied to installation of app " + appId.toString())
                }
            }
        }
    }

    private fun ensureRoleIsNotAdminRole(organizationId: UUID, roleId: UUID) {
        val adminRoleAclKey = organizations.getAdminRoleAclKey(organizationId)
        Preconditions.checkArgument(
                roleId != adminRoleAclKey[1],
                "Role " + adminRoleAclKey.toString()
                        + " cannot be deleted because it is the organization's admin role."
        )
    }

}