package com.openlattice.collaborations.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.collaborations.Collaboration
import com.openlattice.collaborations.CollaborationService
import com.openlattice.collaborations.CollaborationsApi
import com.openlattice.collaborations.CollaborationsApi.Companion.ALL_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.COLLABORATION_ID_PARAM
import com.openlattice.collaborations.CollaborationsApi.Companion.COLLABORATION_ID_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.CONTROLLER
import com.openlattice.collaborations.CollaborationsApi.Companion.DATABASE_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.DATA_SETS_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.DATA_SET_ID_PARAM
import com.openlattice.collaborations.CollaborationsApi.Companion.DATA_SET_ID_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.ID_PARAM
import com.openlattice.collaborations.CollaborationsApi.Companion.ORGANIZATIONS_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.ORGANIZATION_ID_PARAM
import com.openlattice.collaborations.CollaborationsApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.PROJECT_PATH
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
    justification = "Allowing redundant kotlin null check on lateinit variables"
)
class CollaborationsController : AuthorizingComponent, CollaborationsApi {

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Inject
    private lateinit var collaborationService: CollaborationService

    @Timed
    @GetMapping(
        value = [ALL_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAllCollaborations(): Iterable<Collaboration> {
        val authorizedCollaborationIds = getAllAuthorizedCollaborationIds()
        return collaborationService.getCollaborations(authorizedCollaborationIds).values
    }

    @Timed
    @GetMapping(
            value = ["", "/"],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCollaborations(@RequestParam( ID_PARAM ) ids: Set<UUID>): Map<UUID, Collaboration> {
        val authorizedCollaborationIds = getAllAuthorizedCollaborationIds().intersect(ids)
        return collaborationService.getCollaborations(authorizedCollaborationIds)
    }

    @Timed
    @GetMapping(
        value = [COLLABORATION_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCollaboration(@PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID): Collaboration {
        ensureReadAccess(AclKey(collaborationId))
        return collaborationService.getCollaboration(collaborationId)
    }

    @Timed
    @GetMapping(
        value = [ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCollaborationsWithOrganization(
        @PathVariable(ORGANIZATION_ID_PARAM) organizationId: UUID
    ): Iterable<Collaboration> {
        ensureReadAccess(AclKey(organizationId))
        val collaborations = collaborationService.getCollaborationsIncludingOrg(organizationId)
        val authorizedIds = filterToAuthorizedIds(collaborations.map { it.id })
        return collaborations.filter { authorizedIds.contains(it.id) }
    }

    @Timed
    @PostMapping(
        value = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun createCollaboration(@RequestBody collaboration: Collaboration): UUID {
        return collaborationService.createCollaboration(collaboration, Principals.getCurrentUser())
    }

    @Timed
    @DeleteMapping(
        value = [COLLABORATION_ID_PATH]
    )
    override fun deleteCollaboration(@PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID) {
        ensureOwnerAccess(AclKey(collaborationId))
        collaborationService.deleteCollaboration(collaborationId)
    }

    @Timed
    @PatchMapping(
        value = [COLLABORATION_ID_PATH + ORGANIZATIONS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun addOrganizationsToCollaboration(
        @PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @RequestBody organizationIds: Set<UUID>
    ) {
        ensureOwnerAccess(AclKey(collaborationId))
        collaborationService.addOrganizationIdsToCollaboration(collaborationId, organizationIds)
    }

    @Timed
    @DeleteMapping(
        value = [COLLABORATION_ID_PATH + ORGANIZATIONS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun removeOrganizationsFromCollaboration(
        @PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @RequestBody organizationIds: Set<UUID>
    ) {
        ensureOwnerAccess(AclKey(collaborationId))
        collaborationService.removeOrganizationIdsFromCollaboration(collaborationId, organizationIds)
    }

    @Timed
    @GetMapping(
        value = [COLLABORATION_ID_PATH + DATABASE_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCollaborationDatabaseInfo(
        @PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID
    ): OrganizationDatabase {
        ensureReadAccess(AclKey(collaborationId))
        return collaborationService.getDatabaseInfo(collaborationId)
    }

    @Timed
    @PatchMapping(
        value = [COLLABORATION_ID_PATH + DATABASE_PATH],
        consumes = [MediaType.TEXT_PLAIN_VALUE]
    )
    override fun renameCollaborationDatabase(
        @PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @RequestBody name: String
    ) {
        ensureOwnerAccess(AclKey(collaborationId))
        collaborationService.renameDatabase(collaborationId, name)
    }

    @Timed
    @PatchMapping(
        value = [COLLABORATION_ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + DATA_SET_ID_PATH]
    )
    override fun addDataSetToCollaboration(
        @PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @PathVariable(ORGANIZATION_ID_PARAM) organizationId: UUID,
        @PathVariable(DATA_SET_ID_PARAM) dataSetId: UUID
    ) {
        ensureReadAccess(AclKey(collaborationId))
        ensureReadAccess(AclKey(organizationId))
        ensureOwnerAccess(AclKey(dataSetId))
        collaborationService.projectTableToCollaboration(collaborationId, organizationId, dataSetId)
    }

    @Timed
    @DeleteMapping(
        value = [COLLABORATION_ID_PATH + PROJECT_PATH + ORGANIZATION_ID_PATH + DATA_SET_ID_PATH]
    )
    override fun removeDataSetFromCollaboration(
        @PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID,
        @PathVariable(ORGANIZATION_ID_PARAM) organizationId: UUID,
        @PathVariable(DATA_SET_ID_PARAM) dataSetId: UUID
    ) {
        ensureReadAccess(AclKey(collaborationId))
        ensureReadAccess(AclKey(organizationId))
        ensureOwnerAccess(AclKey(dataSetId))
        collaborationService.removeProjectedTableFromCollaboration(collaborationId, organizationId, dataSetId)
    }

    @Timed
    @GetMapping(
        value = [ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH + DATA_SETS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getOrganizationCollaborationDataSets(
        @PathVariable(ORGANIZATION_ID_PARAM) organizationId: UUID
    ): Map<UUID, List<UUID>> {
        val authorizedCollaborationIds = getCollaborationsWithOrganization(organizationId).map { it.id }
        return collaborationService
            .getProjectedTableIdsInCollaborationsAndOrganizations(
                authorizedCollaborationIds,
                setOf(organizationId)
            ) {
                it.key.collaborationId
            }
            .mapValues {
                filterToAuthorizedIds(it.value).toList()
            }
    }

    @Timed
    @GetMapping(
        value = [COLLABORATION_ID_PATH + DATA_SETS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCollaborationDataSets(
        @PathVariable(COLLABORATION_ID_PARAM) collaborationId: UUID
    ): Map<UUID, List<UUID>> {
        ensureReadAccess(AclKey(collaborationId))
        val collaboration = collaborationService.getCollaboration(collaborationId)
        val authorizedOrgIds = filterToAuthorizedIds(collaboration.organizationIds)
        return collaborationService
            .getProjectedTableIdsInCollaborationsAndOrganizations(
                setOf(collaborationId),
                authorizedOrgIds
            ) {
                it.value.organizationId
            }
            .mapValues {
                filterToAuthorizedIds(it.value).toList()
            }
    }

    @Timed
    @PostMapping(
        value = [DATA_SETS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCollaborationsWithDataSets(@RequestBody dataSetIds: Set<UUID>): Map<UUID, List<Collaboration>> {
        val authorizedDataSetIds = filterToAuthorizedIds(dataSetIds)
        val authorizedCollaborationIds = getAllAuthorizedCollaborationIds()
        val test = collaborationService.getCollaborationIdsWithProjectionsForTables(
            authorizedDataSetIds,
            authorizedCollaborationIds
        )
        return test.mapValues { collaborationService.getCollaborations(it.value.toSet()).values.toList() }

    }

    private fun getAllAuthorizedCollaborationIds(): Set<UUID> {
        return authorizations.getAuthorizedObjectsOfType(
            Principals.getCurrentPrincipals(),
            SecurableObjectType.Collaboration,
            EnumSet.of(Permission.READ)
        ).map { it.first() }.collect(Collectors.toSet())
    }

    private fun filterToAuthorizedIds(ids: Iterable<UUID>): Set<UUID> {
        return authorizations.accessChecksForPrincipals(
            ids.map { AccessCheck(AclKey(it), EnumSet.of(Permission.READ)) }.toSet(),
            Principals.getCurrentPrincipals()
        ).filter { it.permissions.getValue(Permission.READ) }.map { it.aclKey.first() }.collect(Collectors.toSet())
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }
}
