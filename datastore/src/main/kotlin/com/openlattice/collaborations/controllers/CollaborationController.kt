package com.openlattice.collaborations.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.collaborations.Collaboration
import com.openlattice.collaborations.CollaborationService
import com.openlattice.collaborations.CollaborationsApi
import com.openlattice.collaborations.CollaborationsApi.Companion.CONTROLLER
import com.openlattice.collaborations.CollaborationsApi.Companion.DATABASE_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.ID
import com.openlattice.collaborations.CollaborationsApi.Companion.ID_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.ORGANIZATIONS_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.ORGANIZATION_ID
import com.openlattice.collaborations.CollaborationsApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.PROJECT_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.TABLE_ID
import com.openlattice.collaborations.CollaborationsApi.Companion.TABLE_ID_PATH
import com.openlattice.organizations.OrganizationDatabase
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
        justification = "Allowing redundant kotlin null check on lateinit variables")
class CollaborationController : AuthorizingComponent, CollaborationsApi {

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Inject
    private lateinit var collaborationService: CollaborationService

    @Timed
    @GetMapping(value = ["", "/"], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getCollaborations(): Iterable<Collaboration> {
        val authorizedCollaborationIds = authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.Collaboration,
                EnumSet.of(Permission.READ)
        ).map { it.first() }.collect(Collectors.toSet())

        return collaborationService.getCollaborations(authorizedCollaborationIds).values
    }

    @Timed
    @PostMapping(value = ["", "/"], consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun createCollaboration(@RequestBody collaboration: Collaboration): UUID {
        return collaborationService.createCollaboration(collaboration, Principals.getCurrentUser())
    }

    @Timed
    @GetMapping(value = [ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getCollaboration(@PathVariable(ID) id: UUID): Collaboration {
        ensureReadAccess(AclKey(id))
        return collaborationService.getCollaboration(id)
    }

    @Timed
    @GetMapping(value = [ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getCollaborationsIncludingOrganization(@PathVariable(ORGANIZATION_ID) organizationId: UUID): Iterable<Collaboration> {
        ensureReadAccess(AclKey(organizationId))
        val collaborations = collaborationService.getCollaborationsIncludingOrg(organizationId)

        val authorizedIds = authorizations.accessChecksForPrincipals(
                collaborations.map { AccessCheck(AclKey(it.id), EnumSet.of(Permission.READ)) }.toSet(),
                Principals.getCurrentPrincipals()
        ).filter { it.permissions.getValue(Permission.READ) }.map { it.aclKey.first() }.collect(Collectors.toSet())

        return collaborations.filter { authorizedIds.contains(it.id) }
    }

    @Timed
    @DeleteMapping(value = [ID_PATH])
    override fun deleteCollaboration(@PathVariable(ID) id: UUID) {
        ensureOwnerAccess(AclKey(id))
        collaborationService.deleteCollaboration(id)
    }

    @Timed
    @PostMapping(value = [ID_PATH + ORGANIZATIONS_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun addOrganizationIdsToCollaboration(@PathVariable(ID) id: UUID, @RequestBody organizationIds: Set<UUID>) {
        ensureOwnerAccess(AclKey(id))
        collaborationService.addOrganizationIdsToCollaboration(id, organizationIds)
    }

    @Timed
    @DeleteMapping(value = [ID_PATH + ORGANIZATIONS_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun removeOrganizationIdsFromCollaboration(@PathVariable(ID) id: UUID, @RequestBody organizationIds: Set<UUID>) {
        ensureOwnerAccess(AclKey(id))
        collaborationService.removeOrganizationIdsFromCollaboration(id, organizationIds)
    }

    @Timed
    @GetMapping(value = [ID_PATH + DATABASE_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getCollaborationDatabaseInfo(@PathVariable(ID) id: UUID): OrganizationDatabase {
        ensureReadAccess(AclKey(id))
        return collaborationService.getDatabaseInfo(id)
    }

    @Timed
    @PatchMapping(value = [ID_PATH, DATABASE_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun renameDatabase(@PathVariable(ID) id: UUID, @RequestBody newDatabaseName: String) {
        ensureOwnerAccess(AclKey(id))
        collaborationService.renameDatabase(id, newDatabaseName)

    }

    @Timed
    @GetMapping(value = [ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + TABLE_ID_PATH])
    override fun projectTableToCollaboration(
            @PathVariable(ID) collaborationId: UUID,
            @PathVariable(ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(TABLE_ID) tableId: UUID
    ) {
        ensureReadAccess(AclKey(collaborationId))
        ensureReadAccess(AclKey(organizationId))
        ensureOwnerAccess(AclKey(tableId))

        collaborationService.projectTableToCollaboration(collaborationId, organizationId, tableId)
    }

    @Timed
    @DeleteMapping(value = [ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + TABLE_ID_PATH])
    override fun removeProjectedTableFromCollaboration(
            @PathVariable(ID) collaborationId: UUID,
            @PathVariable(ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(TABLE_ID) tableId: UUID
    ) {
        ensureReadAccess(AclKey(collaborationId))
        ensureReadAccess(AclKey(organizationId))
        ensureOwnerAccess(AclKey(tableId))

        collaborationService.removeProjectedTableFromCollaboration(collaborationId, organizationId, tableId)

    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }

}