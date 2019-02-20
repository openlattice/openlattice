package com.openlattice.linking.controllers

import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.linking.LinkingConfiguration
import com.openlattice.linking.LinkingQueryService
import com.openlattice.linking.RealtimeLinkingApi
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(RealtimeLinkingApi.CONTROLLER)
class RealtimeLinkingController(


        lc: LinkingConfiguration
) : RealtimeLinkingApi, AuthorizingComponent {
    @Inject
    private lateinit var lqs: LinkingQueryService

    @Inject
    private lateinit var authz: AuthorizationManager

    @Inject
    private lateinit var edm: EdmManager

    private lateinit var entitySetBlacklist: Set<UUID>
    private lateinit var whitelist: Optional<Set<UUID>>
    private lateinit var linkableTypes: Set<UUID>

    @Inject
    fun setLinkingConfiguration(lc: LinkingConfiguration) {
        entitySetBlacklist = lc.blacklist
        whitelist = lc.whitelist
        linkableTypes = edm.getEntityTypeUuids(lc.entityTypes)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authz
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeLinkingController::class.java)
    }

    @RequestMapping(
            path = [RealtimeLinkingApi.FINISHED + RealtimeLinkingApi.SET],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getLinkingFinishedEntitySets(): Set<UUID> {
        ensureAdminAccess()
        val linkableEntitySets = lqs
                .getLinkableEntitySets(linkableTypes, entitySetBlacklist, whitelist.orElse(setOf()))
                .toSet()
        val entitySetsNeedLinking = lqs.getEntitiesNotLinked(linkableEntitySets).map { it.first }
        return linkableEntitySets.minus(entitySetsNeedLinking)
    }
}