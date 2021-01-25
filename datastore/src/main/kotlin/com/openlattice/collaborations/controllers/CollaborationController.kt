package com.openlattice.datastore.services.com.openlattice.collaborations.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.collaborations.Collaboration
import com.openlattice.collaborations.CollaborationsApi
import com.openlattice.collaborations.CollaborationsApi.Companion.CONTROLLER
import com.openlattice.collaborations.CollaborationsApi.Companion.ID
import com.openlattice.collaborations.CollaborationsApi.Companion.ID_PATH
import com.openlattice.collaborations.CollaborationsApi.Companion.ORGANIZATIONS_PATH
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
        justification = "Allowing redundant kotlin null check on lateinit variables")
class CollaborationController : AuthorizingComponent, CollaborationsApi {

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Timed
    @GetMapping(value = ["", "/"], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getCollaborations(): Iterable<Collaboration> {
        TODO("Not yet implemented")
    }

    @Timed
    @PostMapping(value = ["", "/"], consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun createCollaboration(@RequestBody collaboration: Collaboration): UUID {
        TODO("Not yet implemented")
    }

    @Timed
    @GetMapping(value = [ID_PATH], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getCollaboration(@PathVariable(ID) id: UUID): Collaboration {
        TODO("Not yet implemented")
    }

    @Timed
    @DeleteMapping(value = [ID_PATH])
    override fun deleteCollaboration(@PathVariable(ID) id: UUID) {
        TODO("Not yet implemented")
    }

    @Timed
    @PostMapping(value = [ID_PATH + ORGANIZATIONS_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun addOrganizationIdsToCollaboration(@PathVariable(ID) id: UUID, @RequestBody organizationIds: Iterable<UUID>) {
        TODO("Not yet implemented")
    }

    @Timed
    @DeleteMapping(value = [ID_PATH + ORGANIZATIONS_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun removeOrganizationIdsFromCollaboration(@PathVariable(ID) id: UUID, @RequestBody organizationIds: Iterable<UUID>) {
        TODO("Not yet implemented")
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }

}